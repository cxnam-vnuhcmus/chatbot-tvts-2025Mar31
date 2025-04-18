import panel as pn
from datetime import datetime, timedelta
import logging
from typing import Dict, Optional
import os
import param
import json
import traceback
import logging
import pandas as pd
import time
from functools import wraps
import sys
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))  
from common.data_manager import DatabaseManager
from common.data_processor import DataProcessor
from common.chroma_manager import ChromaManager
from common.utils import format_content_markdown, format_date
from common.conflict_manager import ConflictManager
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
max_length = int(os.environ.get("MAX_LENGTH"))

pn.extension(
    "texteditor", 
    "tabulator",
    sizing_mode="stretch_width",
    notifications=True
)

class KMSAdmin(param.Parameterized):
    document_type = param.ObjectSelector(default="Import from interface", objects=["Import from interface"])
    status_filter = param.String(default="All")
    
    def __init__(self, username, admin_permissions=False):
        super().__init__()
        self.username = username
        self._document_cache = {}
        self.current_doc_id = None
        self._button_states_cache = {}
        self._chunks_cache = {}
        self._last_loaded_chunks = None
        self._is_loading_chunks = False
        self._is_updating = False
        self._last_update = datetime.now() - timedelta(seconds=30)  
        
        self._user_is_interacting = False  
        self._pause_auto_updates = False  
        self._disable_selection_change = False 
        self._currently_viewing_tab = None  
        self._last_chunks_load_time = 0     
        self._last_conflict_load_time = 0 
    
        self._init_cache()
        
        self.data_manager = DatabaseManager()
        self.data_processor = DataProcessor()
        self.chroma_manager = ChromaManager()
        
        self.init_conflict_components()
        
        self._last_chunk_load_time = 0
        self._chunk_load_debounce = 1000 
        self._chunk_status_cache = {}
        self._last_active_tab = None
        self._last_chunk_status = None
        self._chunk_components = {}
        
        self.unit = self.data_processor.get_user_unit(username)
        
        self.column_titles = {
            'id': 'Mã tài liệu',
            'content': 'Nội dung',
            'created_date': 'Ngày tạo',
            'unit': 'Đơn vị',
            'sender': 'Người tạo',
            'approval_status': 'Trạng thái', 
            'is_duplicate': 'Có Trùng lắp',
            'conflict_status': 'Có Mâu thuẫn',
        }


        try:
            self.all_data = self.data_manager.get_all_documents()
            if self.all_data is None or len(self.all_data) == 0:
                logger.warning("Không tìm thấy dữ liệu ban đầu, khởi tạo DataFrame trống")
                self.all_data = pd.DataFrame(columns=[
                    'id', 'content', 'created_date', 'unit', 'sender', 
                    'approval_status', 
                    'is_duplicate', 
                    'conflict_status'
                ])
        except Exception as e:
            logger.error(f"Lỗi khi lấy dữ liệu ban đầu: {str(e)}")
            logger.error(traceback.format_exc())
            self.all_data = pd.DataFrame(columns=[
                'id', 'content', 'created_date', 'unit', 'sender', 
                'approval_status', 
                'is_duplicate', 'conflict_status'
            ])
        
        self._format_initial_data()
        self.data_manager.fix_conflict_status_values()
        
        self.displayed_columns = [
            'id', 'content', 'created_date', 'unit', 'sender', 
            'approval_status', 
            'is_duplicate', 
            'conflict_status'
        ]

        self.loading_indicator = pn.indicators.LoadingSpinner(value=False, size=20)
        self.chunks_loading = pn.indicators.LoadingSpinner(value=False, size=20)
        self.save_indicator = pn.indicators.LoadingSpinner(value=False, size=20)
                
        self.chunk_info_container = pn.Column(
            pn.pane.Markdown("", styles={'color': 'blue'}),
            visible=False,
            sizing_mode='stretch_width'
        )
        
        self.chunk_error_container = pn.Column(
            pn.pane.Markdown("", styles={'color': 'red'}),
            visible=False,
            sizing_mode='stretch_width'
        )

        self.error_message = pn.pane.Markdown("", styles={'color': 'red'})
        self.info_message = pn.pane.Markdown("", styles={'color': 'blue'})

        self.create_buttons()
        self.create_tables()
        self.create_views()
        self.create_widgets()
        self.create_layout()
        
        self.setup_auto_update(period=5000) 
        
        try:
            if hasattr(self, 'tabs'):
                self.tabs.visible = False
                
            if hasattr(self, 'save_button'):
                self.save_button.visible = False
                
        except Exception as e:
            logger.error(f"Error setting initial visibility: {str(e)}")
        
        self._is_updating = False 
        self.update_table() 
        self.setup_periodic_conflict_check(period=300000)  
        self.setup_auto_update(period=5000) 
        self.setup_conflict_analysis_monitoring(period=2000)  
    
    
    def setup_periodic_conflict_check(self, period=300000):  
        """
        Setup a periodic callback to check and reanalyze conflicts for documents with external conflicts
        
        Args:
            period (int): Period in milliseconds between checks
        """
        try:
            self._last_conflict_check = datetime.now() - timedelta(seconds=10)
            
            if hasattr(self, 'conflict_check_callback') and self.conflict_check_callback is not None:
                try:
                    self.conflict_check_callback.stop()
                except Exception:
                    pass
                    
            try:
                self.conflict_check_callback = pn.state.add_periodic_callback(
                    self.check_external_conflicts,
                    period
                )
                logger.info(f"Setup periodic conflict check every {period/1000} seconds")
            except Exception as e:
                logger.error(f"Could not set conflict check callback: {str(e)}")
                try:
                    if hasattr(pn.state, 'onload'):
                        pn.state.onload(lambda: pn.state.add_periodic_callback(self.check_external_conflicts, period))
                except:
                    pass
        except Exception as e:
            logger.error(f"Error setting up periodic conflict check: {str(e)}")

    def check_external_conflicts(self, force_check=False):
        """Periodically check and reanalyze external conflicts"""
        try:
            if hasattr(self, '_is_checking_conflicts') and self._is_checking_conflicts and not force_check:
                return
                
            self._is_checking_conflicts = True
            logger.info("Checking for documents with external conflicts")
            
            try:
                query = """
                    SELECT d.id, d.duplicate_group_id, d.last_conflict_check 
                    FROM documents d
                    WHERE d.duplicate_group_id IS NOT NULL
                    AND (
                        d.has_conflicts = true 
                        OR d.last_conflict_check IS NULL
                        OR d.last_conflict_check < NOW() - INTERVAL '1 hour'
                    )
                    ORDER BY 
                        CASE WHEN d.has_conflicts = true THEN 0 ELSE 1 END,
                        d.last_conflict_check ASC NULLS FIRST
                    LIMIT 20
                """
                
                documents = self.data_manager.execute_with_retry(query, fetch=True)
                
                if not documents:
                    logger.info("No documents found that need conflict reanalysis")
                    self._is_checking_conflicts = False
                    return
                    
                logger.info(f"Found {len(documents)} documents that need conflict reanalysis")
                
                groups_to_analyze = {}
                for doc in documents:
                    doc_id = doc[0]
                    group_id = doc[1]
                    
                    if group_id and group_id not in groups_to_analyze:
                        groups_to_analyze[group_id] = doc_id
                
                current_group_processed = False
                if self.current_doc_id:
                    current_doc = self.data_manager.get_document_by_id(self.current_doc_id)
                    if current_doc:
                        current_group = current_doc.get('duplicate_group_id')
                        if current_group and current_group in groups_to_analyze:
                            self._process_conflict_group(current_group, groups_to_analyze[current_group])
                            del groups_to_analyze[current_group]
                            current_group_processed = True
                
                if not hasattr(self, 'conflict_manager'):
                    self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                
                for group_id, representative_doc_id in groups_to_analyze.items():
                    self._process_conflict_group(group_id, representative_doc_id)
                    
                if self.current_doc_id and self.tabs.active == 3 and (current_group_processed or force_check):
                    logger.info(f"Refreshing conflicts view for current document {self.current_doc_id}")
                    self.load_conflicts_data(self.current_doc_id)
                
            except Exception as query_error:
                logger.error(f"Error querying documents with external conflicts: {str(query_error)}")
                
        except Exception as e:
            logger.error(f"Error in periodic conflict check: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            self._is_checking_conflicts = False
    
    def _process_conflict_group(self, group_id, representative_doc_id):
        """Process conflicts for a group of documents"""
        try:
            logger.info(f"Reanalyzing conflicts for group {group_id} (representative doc: {representative_doc_id})")
            
            self.conflict_manager.sync_group_conflicts_by_group(group_id)
            
            group_docs = self.data_manager.get_documents_in_group(group_id)
            if group_docs:
                first_doc = group_docs[0]['id']
                logger.info(f"Performing detailed analysis on document {first_doc}")
                self.conflict_manager.analyze_document(first_doc)
                
                logger.info(f"Syncing results to all documents in group {group_id}")
                self.conflict_manager.sync_group_conflicts_by_group(group_id)
                
                for doc in group_docs:
                    self.data_manager.update_document_status(doc['id'], {
                        'last_conflict_check': datetime.now().isoformat()
                    })
                    
        except Exception as group_error:
            logger.error(f"Error reanalyzing conflicts for group {group_id}: {str(group_error)}")
    
    def show_notification(self, message, alert_type="info", duration=3000):
        try:
            current_time = time.time()
            notification_key = f"{alert_type}:{message}"
            
            if hasattr(self, '_last_notifications'):
                if notification_key in self._last_notifications:
                    if current_time - self._last_notifications[notification_key] < 2:
                        return
            else:
                self._last_notifications = {}
                
            self._last_notifications[notification_key] = current_time
            
            if len(self._last_notifications) > 20:
                oldest_keys = sorted(self._last_notifications.keys(), 
                                key=lambda k: self._last_notifications[k])[:10]
                for key in oldest_keys:
                    del self._last_notifications[key]
            
            if pn.state.notifications is not None:
                if alert_type == "success":
                    pn.state.notifications.success(message, duration=duration)
                elif alert_type == "error":
                    pn.state.notifications.error(message, duration=duration)
                elif alert_type == "warning":
                    pn.state.notifications.warning(message, duration=duration)
                else:
                    pn.state.notifications.info(message, duration=duration)
            else:
                logger.info(f"Notification ({alert_type}): {message}")
                if hasattr(self, 'info_message'):
                    prefix = {
                        "info": "ℹ️ ",
                        "success": "✅ ",
                        "error": "❌ ",
                        "warning": "⚠️ "
                    }.get(alert_type, "")
                    self.info_message.object = f"{prefix}{message}"
                    self.info_message.styles = {"color": {
                        "info": "blue",
                        "success": "green", 
                        "error": "red",
                        "warning": "orange"
                    }.get(alert_type, "blue")}
                    self.info_message.visible = True
        except Exception as e:
            logger.error(f"Unable to display message: {str(e)}")
            
    def _init_cache(self):
        try:
            self._document_cache = {}
            self._chunks_cache = {}
            self._cache_size = 30 
            self._cache_access_time = {}
            self._cache_priority = {}  
            
            self._computation_cache = {}
            
            self._last_loaded_chunks = None
            self._is_loading_chunks = False
            self._is_updating = False
            self._last_chunk_load_time = 0
            self._chunk_load_debounce = 1000
            self._last_update = datetime.now()
            
            logger.info("Cache system initialized with size 30")
        except Exception as e:
            logger.error(f"Error initializing cache: {str(e)}")
            self._chunks_cache = {}
            self._document_cache = {}

    def _update_cache(self, key, value, priority=1):
        """
        Update cache với cơ chế LRU và ưu tiên
        
        Args:
            key (str): Cache key
            value (any): Giá trị cần cache
            priority (int): Mức ưu tiên (1-5, càng cao càng ưu tiên)
        """
        try:
            self._chunks_cache[key] = value
            self._cache_access_time[key] = time.time()
            self._cache_priority[key] = priority
            
            if len(self._chunks_cache) > self._cache_size:
                cache_scores = {}
                for k in self._chunks_cache.keys():
                    access_time = self._cache_access_time.get(k, 0)
                    priority = self._cache_priority.get(k, 1)
                    cache_scores[k] = access_time + (100 * priority)
                
                if cache_scores:
                    min_key = min(cache_scores, key=cache_scores.get)
                    if min_key in self._chunks_cache:
                        del self._chunks_cache[min_key]
                    if min_key in self._cache_access_time:
                        del self._cache_access_time[min_key]
                    if min_key in self._cache_priority:
                        del self._cache_priority[min_key]
                        
                    logger.info(f"Removed cache item with lowest score: {min_key}")
        except Exception as e:
            logger.error(f"Error updating cache: {str(e)}")
        
        
   
    def _get_from_cache(self, key):
        """
        Get data from cache and update access time

        Args:
        key (str): Cache key to get

        Returns:
        any: Value from cache or None if not found
        """
        try:
            if key in self._chunks_cache:
                self._cache_access_time[key] = time.time()
                return self._chunks_cache[key]
            return None
        except Exception as e:
            logger.error(f"Error getting from cache: {str(e)}")
            return None     
    
    def _format_initial_data(self):
        try:
            if self.all_data is None or len(self.all_data) == 0:
                return
                
            if 'is_duplicate' in self.all_data.columns:
                self.all_data['is_duplicate'] = self.all_data['is_duplicate'].apply(
                    lambda x: "Có trùng lắp" if x else "Không trùng lắp"
                )

            if 'conflict_status' in self.all_data.columns:
                self.all_data['conflict_status'] = self.all_data['conflict_status'].apply(
                    lambda x: "Không mẫu thuẫn" if x == "No Conflict" else "Có mâu thuẫn"
                )

        except Exception as e:
            logger.error(traceback.format_exc())

    
    def setup_auto_update(self, period=7000): 
        try:
            self._last_update = datetime.now() - timedelta(seconds=10)  
            self._is_updating = False
            
            for callback_name in ['update_callback', 'chunks_callback', 'reanalysis_check_callback']:
                if hasattr(self, callback_name) and getattr(self, callback_name) is not None:
                    try:
                        getattr(self, callback_name).stop()
                    except Exception:
                        pass
            
            def combined_status_check():
                if hasattr(self, 'tabs') and self.tabs.visible:
                    active_tab = self.tabs.active if hasattr(self, 'tabs') else None
                    
                    current_time = time.time()
                    last_table_update = getattr(self, '_last_table_update', 0)
                    if current_time - last_table_update > 10:  
                        self._last_table_update = current_time
                        self._throttled_update()
                    
                    if active_tab == 1 and self.current_doc_id:
                        self.check_chunk_status(force_check=False)
                    
                    if active_tab == 3 and self.current_doc_id:
                        self.check_reanalysis_needed()
            
            try:
                self.status_check_callback = pn.state.add_periodic_callback(
                    combined_status_check,
                    period  
                )
            except Exception as e:
                logger.error(f"Could not set combined status callback: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error setting auto update: {str(e)}")
            logger.error(traceback.format_exc())
    
    
    def init_conflict_components(self):
        try:
            self.conflict_info_container = pn.Column(
                pn.pane.Markdown("", styles={'color': 'blue'}),
                visible=False,
                sizing_mode='stretch_width'
            )
            
            self.conflict_error_container = pn.Column(
                pn.pane.Markdown("", styles={'color': 'red'}),
                visible=False,
                sizing_mode='stretch_width'
            )
            
            self.content_conflicts_container = pn.Column(name="Mâu thuẫn nội dung", sizing_mode='stretch_width')
            self.internal_conflicts_container = pn.Column(name="Mâu thuẫn nội bộ", sizing_mode='stretch_width')
            self.external_conflicts_container = pn.Column(name="Mâu thuẫn ngoại bộ", sizing_mode='stretch_width')
            
            self.conflict_tabs = pn.Tabs(
                ("Mâu thuẫn nội dung", self.content_conflicts_container),
                ("Mâu thuẫn nội bộ", self.internal_conflicts_container),
                ("Mâu thuẫn ngoại bộ", self.external_conflicts_container)
            )
            
            self.conflict_summary = pn.pane.Markdown(
                "",
                styles={
                    'background': '#f0f9ff',
                    'padding': '10px',
                    'border-radius': '4px',
                    'border': '1px solid #bae6fd',
                    'margin-bottom': '10px',
                },
                sizing_mode='stretch_width'
            )
            
            self.fix_conflicts_button = pn.widgets.Button(
                name="Phân tích mâu thuẫn", 
                button_type="primary",
                button_style="solid",
                width=180,
                height=40,
                styles={
                    'font-weight': 'bold',
                    'font-size': '14px',
                    'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
                }
            )
            self.fix_conflicts_button.on_click(lambda event: self.request_conflict_analysis(self.current_doc_id))
            
            deepseek_status = pn.pane.Markdown(
                "**Model phân tích:** OpenAI GPT (online)",
                styles={
                    'font-size': '12px',
                    'color': '#4a5568',
                    'text-align': 'right',
                    'margin-top': '5px',
                    'margin-bottom': '15px',
                    'padding': '5px 10px',
                    'background': '#edf2f7',
                    'border-radius': '4px',
                    'display': 'inline-block'
                }
            )
                        
            model_row = pn.Row(deepseek_status, align='end')
            
            self.conflicts_container = pn.Column(
                pn.Row(
                    pn.pane.Markdown("### Thông tin mâu thuẫn", styles={
                        'color': '#2c5282',
                        'font-size': '18px',
                        'margin-bottom': '5px'
                    }),
                    pn.Spacer(width=20),
                    self.fix_conflicts_button,
                    align='center'
                ),
                self.conflict_summary,
                model_row,
                self.conflict_info_container,
                self.conflict_error_container,
                self.conflict_tabs,
                sizing_mode='stretch_width'
            )
            
        except Exception as e:
            logger.error(f"Error initializing conflicting components: {str(e)}")
        
    def _throttled_update(self):
        try:
            current_time = datetime.now()
            
            if (hasattr(self, '_user_is_interacting') and self._user_is_interacting) or \
            (hasattr(self, '_pause_auto_updates') and self._pause_auto_updates):
                return
                
            if hasattr(self, '_is_updating') and self._is_updating:
                return
                
            elapsed_seconds = (current_time - self._last_update).total_seconds()
            if elapsed_seconds < 3:  
                logger.info(f"Chỉ mới {elapsed_seconds}s từ lần cập nhật cuối, bỏ qua")
                return
                
            self._is_updating = True
            
            try:
                if self.current_doc_id and hasattr(self, 'tabs') and self.tabs.visible:
                    self._update_current_document_status()
                else:
                    self.update_table()
            finally:
                self._is_updating = False
                self._last_update = datetime.now()
                
        except Exception as e:
            self._is_updating = False
            logger.error(f"Lỗi trong _throttled_update: {str(e)}")
            logger.error(traceback.format_exc())
    
    def _update_current_document_status(self):
        try:
            if not self.current_doc_id:
                return
                
            document = self.data_manager.get_document_by_id(self.current_doc_id)
            if not document:
                return
                
            for idx, row in self.all_data.iterrows():
                if row['id'] == self.current_doc_id:
                    if 'approval_status' in document:
                        self.all_data.at[idx, 'approval_status'] = document['approval_status']
                        
                    if 'conflict_status' in document and 'conflict_status' in self.all_data.columns:
                        self.all_data.at[idx, 'conflict_status'] = document['conflict_status']
                        
                    if self.data_table.selection and self.data_table.selection[0] == idx:
                        self.update_button_states(self.all_data.iloc[idx])
                        
                    break
                    
        except Exception as e:
            logger.error(f"Lỗi cập nhật trạng thái tài liệu: {str(e)}")
      
    def create_buttons(self):
        button_width = 80
        
        self.approve_button = pn.widgets.Button(
            name="Duyệt",
            button_type="success",
            button_style="solid", 
            width=button_width,
            visible=True,
            disabled=True
        )
    
        self.reject_button = pn.widgets.Button(
            name="Từ chối",
            button_type="danger", 
            button_style="solid",
            width=button_width,
            visible=True,
            disabled=True
        )
        
        self.delete_button = pn.widgets.Button(
            name="Xóa",
            button_type="warning",
            button_style="solid",
            width=button_width,
            visible=True,
            disabled=True
        )
        self.save_button = pn.widgets.Button(
            name="Lưu thay đổi",
            button_type="primary",
            button_style="solid",
            width=120,
            visible=True  
        )

        self.save_group = pn.Row(
            self.save_button,
            self.save_indicator,
            align='center'
        )
        

        self.approve_button.on_click(self.approve_document)
        self.reject_button.on_click(self.reject_document)
        self.delete_button.on_click(self.confirm_delete)
    
    def update_button_states(self, selected_row=None):
        try:
            if selected_row is None:
                self.approve_button.disabled = True
                self.reject_button.disabled = True
                self.delete_button.disabled = True
                return

            doc_unit = selected_row['unit']
            user_unit = self.unit if hasattr(self, 'unit') and self.unit else 'unit1'
        
            if doc_unit != user_unit:
                self.approve_button.disabled = True  
                self.reject_button.disabled = True
                self.delete_button.disabled = True  
                logger.info(f"Disabling buttons due to unit mismatch: {doc_unit} != {user_unit}")
                return

            approval_status = selected_row['approval_status']
            logger.info(f"Current approval status: {approval_status}")
            
            if approval_status == 'Approved':
                self.approve_button.disabled = True
                self.reject_button.disabled = False
                self.delete_button.disabled = False 
            elif approval_status == 'Rejected':
                self.approve_button.disabled = False  
                self.reject_button.disabled = True
                self.delete_button.disabled = False
            else:  
                self.approve_button.disabled = False
                self.reject_button.disabled = False
                self.delete_button.disabled = False
                
        except Exception as e:
            logger.error(f"Error updating button states: {str(e)}")
            logger.error(traceback.format_exc())
            self.approve_button.disabled = True
            self.reject_button.disabled = True
            self.delete_button.disabled = True
        
    def create_tables(self):
        self.similar_docs_table = pn.widgets.Tabulator(
            pagination='local',
            page_size=5,
            selectable=1,
            height=250,
            sizing_mode="stretch_width",
            show_index=False,
            theme='bootstrap5',
            theme_classes=['table-striped', 'table-bordered']
        )

        self.conflicts_table = pn.widgets.Tabulator(
            pagination='local',
            page_size=5,
            selectable=1,
            height=250,
            sizing_mode="stretch_width",
            show_index=False,
            theme='bootstrap5',
            theme_classes=['table-striped', 'table-bordered']
        )

    def create_widgets(self):
        self.data_filters = {
            'id': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo ID'},
            'content': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo nội dung'},
            'created_date': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo ngày'},
            'unit': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo đơn vị'},
            'sender': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo người tạo'},
            'approval_status': {'type': 'input', 'func': 'like', 'placeholder': ''},
            'is_duplicate': {'type': 'input', 'func': 'like', 'placeholder': ''},
            'conflict_status': {'type': 'input', 'func': 'like', 'placeholder': ''},
        }
        
        options = ["All", "Pending", "Approved", "Rejected"]
        logger.info(f"Using fixed status options: {options}")
        
        self.doc_type_selector = pn.widgets.Select(
            options=options,
            value="All",
            name="Trạng thái",
            width=120,  
        )
        
        if self.all_data is None or len(self.all_data.columns) == 0:
            self.all_data = pd.DataFrame(columns=self.displayed_columns)
        
        available_columns = [col for col in self.displayed_columns if col in self.all_data.columns]
        if len(available_columns) == 0:
            available_columns = self.displayed_columns
        
        
        filtered_data = self.all_data[available_columns] if len(self.all_data) > 0 else pd.DataFrame(columns=available_columns)

        self.column_widths = {
            'id': '10%',               
            'content': '30%',
            'created_date': '10%',   
            'unit': '10%',           
            'approval_status': '10%', 
            'sender': '10%',
            'is_duplicate': '10%',
            'conflict_status': '10%',
        }
        
        dropdown_columns = {
                # 'approval_status': ['Approved', 'Rejected', 'Pending'], 
                # 'is_duplicate': ['Không trùng lắp', 'Có trùng lắp'], 
                # 'conflict_status': ['Không mâu thuẫn', 'Có mâu thuẫn']
            } 
        
        column_width_configs = [
            {
                'field': col,
                'title': self.column_titles.get(col, col),
                'width': self.column_widths.get(col),
                "editable": False,
                'editor': False 
            }
            for col in list(self.column_widths.keys())
        ]

        header_filters = {
            col: {
                'type': 'list' if col in dropdown_columns else 'input',  # Kiểu 'select' hoặc 'input' tùy theo cột
                'values': list(sorted(dropdown_columns[col])) if col in dropdown_columns else []  # Giá trị dropdown nếu có, hoặc danh sách rỗng cho input
            }
            for col in list(self.column_widths.keys())
        } 

        # column_titles = []
        # for col in available_columns:
        #     column_titles.append(self.column_titles.get(col, col))
        
        self.data_table = pn.widgets.Tabulator(
            value=pd.DataFrame(), 
            page_size=20, 
            pagination='remote',
            header_filters=header_filters,
            height=400,
            min_width=1200,
            widths=self.column_widths, 
            disabled=False,
            sizing_mode="stretch_width",
            show_index=False,
            text_align='left',
            theme='bootstrap5',
            theme_classes=['table-striped', 'table-bordered', 'table-hover'],
            selection=[],
            configuration={
                'layout': 'fitColumns',  
                'columns': column_width_configs
            }
        )

        self.doc_type_selector.param.watch(self.filter_by_status, 'value')
        self.data_table.on_click(self.on_row_click)
        self.data_table.param.watch(self.on_selection_change, 'selection')
        
        self.info_message = pn.pane.Markdown("", styles={'color': 'blue'})
        self.error_message = pn.pane.Markdown("", styles={'color': 'red'})
        
    
    def filter_by_status(self, event):
        try:
            status = event.new
            
            if status == "All":
                self.all_data = self.data_manager.get_all_documents()
            else:
                self.all_data = self.data_manager.get_filtered_data(status=status)
            
            self._format_initial_data()
            available_columns = [col for col in self.displayed_columns if col in self.all_data.columns]
            filtered_data = self.all_data[available_columns] if len(self.all_data) > 0 else pd.DataFrame(columns=available_columns)
            self.data_table.value = filtered_data

            self.data_table.selection = []
            self.clear_detail_view()
            
            self.show_notification(f"Tìm thấy {len(self.all_data)} tài liệu với trạng thái {status}", alert_type="info")
            
        except Exception as e:
            logger.error(traceback.format_exc())
            self.show_notification(f"Lỗi khi lọc dữ liệu: {str(e)}", alert_type="error")
                  
    def create_views(self):
        self.detail_view = pn.Column(
            pn.pane.Markdown(""),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown("### Nội dung gốc"),
                    pn.widgets.TextAreaInput(name="", height=300, disabled=True),
                    sizing_mode='stretch_width'
                )
            ),
            height=400,
            sizing_mode='stretch_width'
        )

        self.chunks_container = pn.Column(
            pn.pane.Markdown("### Quản lý Chunks", styles={
                'font-weight': 'bold',
                'margin-bottom': '10px'
            }),
            sizing_mode='stretch_width'
        )

        self.similar_docs_container = pn.Column(
            pn.pane.Markdown("### Tài liệu tương đồng"),
            self.similar_docs_table,
            sizing_mode='stretch_width'
        )

        self.conflicts_container = pn.Column(
            pn.pane.Markdown("### Thông tin mâu thuẫn"),
            self.conflicts_table,
            sizing_mode='stretch_width'
        )

        self.tabs = pn.Tabs(
            ("Chi tiết tài liệu", self.detail_view),
            ("Quản lý Chunks", self.chunks_container),
            ("Tài liệu tương đồng", self.similar_docs_container), 
            ("Thông tin mâu thuẫn", self.conflicts_container),
            sizing_mode='stretch_width'
        )

        self.tabs.param.watch(self.on_tab_change, 'active')

    def create_menu(self):
        menu_items = [
            ("Quản lý tri thức", "/kms_admin"),
            ("Đánh giá hệ thống", "/main")
        ]
        
        current_page = pn.state.location.pathname

        menu_buttons = []
        for name, link in menu_items:
            button_type = "primary" if current_page == link else "default"
            button = pn.widgets.Button(name=name, button_type=button_type, width=140, height=40)
            button.js_on_click(args={"url": link}, code="window.location.href = url;")
            menu_buttons.append(button)
        return pn.Row(*menu_buttons, css_classes=["menu-bar"])
    
    def create_layout(self):        
        custom_css = """
        <style>
            .menu-bar button {
                font-size: 18px;
                transition: transform 0.2s ease-in-out, background-color 0.2s;
            }
            .menu-bar button:hover {
                transform: scale(1.1);
                background-color: #005f99 !important;
                color: white !important;
            }
            .menu-bar .bk-btn-primary {
                background-color: #007acc !important;
                color: white !important;
            }
            .bk-root .fast-list-header h1 {
                font-size: 20px !important;
                font-weight: bold;
            }
            .logout {
                text-align: right;
                font-size: 16px;
                font-weight: bold;
            }
            /* Cải thiện hiệu ứng hover và click cho bảng */
            .tabulator-row {
                transition: background-color 0.2s ease-in-out, transform 0.1s;
            }
            .tabulator-row:hover {
                background-color: #EBF8FF !important;
                cursor: pointer;
            }
            .tabulator-row.tabulator-selected {
                background-color: #BEE3F8 !important;
                transform: scale(1.005);
            }
            .tabulator-row.tabulator-selected:hover {
                background-color: #90CDF4 !important;
            }
            /* Tăng tốc độ phản hồi UI */
            * {
                transition-duration: 0.2s;
            }
        </style>
        """
        
        menu = self.create_menu()
        
        left_column = pn.Column(
            pn.Row(
                self.doc_type_selector,
                pn.Spacer(width=20),
                pn.Row(
                    self.approve_button,
                    pn.Spacer(width=10),  # Khoảng cách nhỏ giữa các nút (tuỳ chỉnh)
                    self.reject_button,
                    pn.Spacer(width=10),
                    self.delete_button,
                    align="end"  # Căn các nút xuống đáy
                ),
                align='start'
            ),
            pn.Spacer(height=20),
            self.data_table,
            self.tabs,
            sizing_mode='stretch_width'
        )
        
        header_row = pn.Row(
            pn.layout.HSpacer(),
            pn.pane.HTML(
                f"""<div style="margin-top: 10px; color: #4A5568; background: #F7FAFC; padding: 10px; font-size: 16px; border-radius: 4px;">
                    👤 {self.username} | <a href="/logout" style="color: #000000; font-weight: bold; text-decoration: none;">Đăng xuất</a>
                </div>""",
            ),
            sizing_mode='stretch_width',
            css_classes=['header']
        )
            
        self.layout = pn.template.FastListTemplate(
            header=[
                pn.pane.HTML(custom_css),
                pn.Row(pn.Spacer(width=50), menu, align="start"),
                header_row
            ],
            title="KMS Admin - HỆ THỐNG QUẢN LÝ TRI THỨC",
            favicon="assets/images/favicon.png",
            main=[left_column],
            theme_toggle=False,
            notifications=pn.state.notifications
        )

    def on_tab_change(self, event):
        try:
            if not self.current_doc_id:
                return
                    
            if getattr(self, '_is_loading_chunks', False) or getattr(self, '_is_loading_tab', False):
                return
                    
            if event.new == getattr(self, '_last_active_tab', None):
                return
                    
            self._last_active_tab = event.new
            
            if event.new != 1: 
                self._last_chunk_status = None
                    
            self._is_loading_tab = True
            
            if event.new in [1, 3]: 
                tab_containers = {
                    1: self.chunks_container if hasattr(self, 'chunks_container') else None,
                    3: self.conflicts_container if hasattr(self, 'conflicts_container') else None
                }
                
                current_container = tab_containers.get(event.new)
                if current_container:
                    current_container.clear()
                    current_container.append(
                        pn.Column(
                            pn.indicators.LoadingSpinner(value=True, size=40),
                            pn.pane.Markdown(
                                "### Đang tải dữ liệu...",
                                styles={
                                    'color': '#4A5568',
                                    'text-align': 'center',
                                    'padding': '20px'
                                }
                            ),
                            align='center'
                        )
                    )
                
                try:
                    if hasattr(pn.state, 'add_timeout_callback'):
                        pn.state.add_timeout_callback(
                            lambda: self.load_tab_data(event.new),
                            100
                        )
                    elif hasattr(pn, 'callbacks') and hasattr(pn.callbacks, 'timeout'):
                        pn.callbacks.timeout(100, lambda: self.load_tab_data(event.new))
                    else:
                        self.load_tab_data(event.new)
                except Exception as async_error:
                    logger.warning(f"Error scheduling tab data loading: {str(async_error)}")
                    self.load_tab_data(event.new)
            else:
                self.load_tab_data(event.new)
                
            self._is_loading_tab = False
                    
        except Exception as e:
            logger.error(f"Error changing tab: {str(e)}")
            logger.error(traceback.format_exc())
            self._is_loading_tab = False
    
    def load_tab_data(self, tab_index):
        """
        Load data for the selected tab based on the tab index.

        This method loads and displays the appropriate data for the tab based on the 
        selected `tab_index`. Each tab corresponds to a specific type of content:

        - Tab 0 (Document Details): Displays detailed information about the selected document.
        - Tab 1 (Chunks Management): Loads and displays chunk data associated with the document.
        - Tab 2 (Similar Documents): Shows documents that are similar to the selected one.
        - Tab 3 (Conflicts Information): Loads and displays information regarding document conflicts.

        The method handles the loading state and ensures that the data is loaded only when
        the document is selected. It also manages error handling and displays relevant
        error messages in case of issues.

        Parameters:
            tab_index (int): The index of the selected tab. 
                            - 0: Detail tab
                            - 1: Chunks tab
                            - 2: Similar documents tab
                            - 3: Conflicts tab

        Returns:
            None
        """
        try:
            if not self.current_doc_id:
                self.show_error_message("Chưa chọn tài liệu")
                return
                    
            self.clear_messages()
                    
            # Detail tab (index 0)
            if tab_index == 0:
                self.update_detail_view()
                self.save_button.visible = True
                        
            # Chunks management tab (index 1)
            elif tab_index == 1:
                if not self._is_loading_chunks:  
                    try:
                        self.chunks_loading.value = True
                        self.chunks_container.clear()
                        self.chunks_container.append(
                            pn.pane.Markdown("### Đang tải chunks...", styles={
                                'text-align': 'center',
                                'padding': '20px'
                            })
                        )
                        self.load_chunks_data(self.current_doc_id)
                    except Exception as e:
                        logger.error(f"Error loading chunks: {str(e)}")
                    finally:
                        self.chunks_loading.value = False
                self.save_button.visible = False
                        
            # Similar documents tab (index 2)
            elif tab_index == 2:
                try:
                    self.similar_docs_container.clear()
                    self.load_similar_documents(self.current_doc_id)
                except Exception as e:
                    self.show_error_message("Lỗi khi tải tài liệu tương đồng")
                finally:
                    self.save_button.visible = False
            
            # Conflicts information tab (index 3)
            elif tab_index == 3:
                try:
                    self.conflicts_container.clear()
                    if not self.current_doc_id:
                        self.show_error_message("Chưa chọn tài liệu")
                        return
                    self.load_conflicts_data(self.current_doc_id)
                except Exception as e:
                    logger.error(f"Error loading conflicts: {str(e)}")
                finally:
                    self.save_button.visible = False
                        
        except Exception as e:
            logger.error(f"Error loading tab data: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_error_message(f"Lỗi khi tải dữ liệu: {str(e)}")
    
    def on_selection_change(self, event):
        try:
            if hasattr(self, '_disable_selection_change') and self._disable_selection_change:
                return
                
            if hasattr(self, '_is_processing_click') and self._is_processing_click:
                return
                
            if hasattr(self, '_last_click_time') and time.time() - self._last_click_time < 0.8:  
                return
                
            if hasattr(self, '_selection_processed') and self._selection_processed:
                self._selection_processed = False
                return
                
            self._is_processing_click = True
            selection = self.data_table.selection
            
            if not selection or len(self.all_data) <= selection[0]:
                self.update_button_states(None)
                self.clear_detail_view()
                self.current_doc_id = None
                self._is_processing_click = False
                return
                
            selected_row = self.all_data.iloc[selection[0]]
            doc_id = selected_row['id']
            
            if doc_id != self.current_doc_id:
                self._user_is_interacting = True
                
                self._pause_auto_updates = True
                
                self.update_button_states(selected_row)
                self.current_doc_id = doc_id
                self.tabs.visible = True
                
                self._is_loading_chunks = False if hasattr(self, '_is_loading_chunks') else False
                self._is_loading_tab = False if hasattr(self, '_is_loading_tab') else False
                self._is_updating = False if hasattr(self, '_is_updating') else False
                
                self.load_tab_data(self.tabs.active)
                
                def resume_updates():
                    self._pause_auto_updates = False
                    self._user_is_interacting = False
                    
                import threading
                threading.Timer(5.0, resume_updates).start()
            
            def reset_processing():
                self._is_processing_click = False
                
            import threading
            threading.Timer(1.0, reset_processing).start()
            
        except Exception as e:
            logger.error(f"Lỗi trong selection change: {str(e)}")
            logger.error(traceback.format_exc())
            self.clear_detail_view()
            self.current_doc_id = None
            self.update_button_states(None)
            self._is_processing_click = False
    
    def load_chunks_data(self, doc_id, page=1, page_size=10):
        """
        Tải chunks data với phân trang
        
        Args:
            doc_id (str): ID của tài liệu
            page (int): Trang cần tải
            page_size (int): Số lượng chunks trên một trang
        """
        try:
            if self._is_loading_chunks:
                return
            
            self._is_loading_chunks = True
            
            self.chunks_container.clear()
            self.clear_messages()
            
            spinner = pn.indicators.LoadingSpinner(value=True, size=50)
            self.chunks_container.append(
                pn.Column(
                    spinner,
                    pn.pane.Markdown("### Đang tải chunks...\nVui lòng chờ trong giây lát...",
                        styles={
                            'color': '#4A5568',
                            'text-align': 'center',
                            'padding': '20px',
                            'background': '#EDF2F7',
                            'border-radius': '8px', 
                            'margin': '20px 0',
                            'font-size': '16px'
                        }
                    ),
                    align='center'
                )
            )
            
            cache_key = f"{doc_id}_chunks"
            cached_chunks = self._get_from_cache(cache_key)
            
            if cached_chunks:
                logger.info(f"Using cached chunks for document {doc_id}")
                self._display_chunks_with_pagination(cached_chunks, doc_id, page, page_size)
                self._is_loading_chunks = False
                return
                
            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                self.chunks_container.clear()
                self.show_error_message(f"Không tìm thấy thông tin tài liệu {doc_id}")
                self._is_loading_chunks = False
                return

            self._last_chunk_load_time = time.time() * 1000
            chunk_status = document.get('chunk_status')
            self._chunk_status_cache[doc_id] = chunk_status
            
            if chunk_status == 'Chunked':
                def load_chunked_data():
                    try:
                        current_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
                        
                        if current_chunks:
                            self._update_cache(cache_key, current_chunks, priority=3)
                            self._last_loaded_chunks = doc_id
                            self._display_chunks_with_pagination(current_chunks, doc_id, page, page_size)
                        else:
                            self.chunks_container.clear()
                            self.show_error_message("Không tìm thấy chunks cho tài liệu này")
                    except Exception as e:
                        logger.error(f"Error loading chunks: {str(e)}")
                        self.chunks_container.clear()
                        self.show_error_message(f"Lỗi khi tải chunks: {str(e)}")
                    finally:
                        self._is_loading_chunks = False
                        
                try:
                    if hasattr(pn.state, 'add_timeout_callback'):
                        pn.state.add_timeout_callback(load_chunked_data, 100)
                    elif hasattr(pn, 'callbacks') and hasattr(pn.callbacks, 'timeout'):
                        pn.callbacks.timeout(100, load_chunked_data)
                    else:
                        load_chunked_data()
                except Exception as async_error:
                    logger.warning(f"Error scheduling chunks loading: {str(async_error)}")
                    load_chunked_data()
            else:
                self._handle_non_chunked_status(chunk_status, doc_id)
                self._is_loading_chunks = False
                
        except Exception as e:
            logger.error(f"Error in load_chunks_data: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_error_message(f"Error loading chunks: {str(e)}")
            self._is_loading_chunks = False
    
    
    def _handle_non_chunked_status(self, status, doc_id):
        """
        Hiển thị trạng thái phù hợp cho các tài liệu không được chia chunks
        
        Args:
            status (str): Trạng thái chunk của tài liệu 
            doc_id (str): ID của tài liệu
        """
        try:
            self.chunks_container.clear()
            
            document = self.data_manager.get_document_by_id(doc_id)
            is_duplicate = document.get('is_duplicate', False)
            original_doc_id = document.get('original_chunked_doc') or document.get('original_doc_id')
            duplicate_group_id = document.get('duplicate_group_id')
            
            if status in ['Pending', 'Processing', 'Chunking', 'Queued']:
                self._show_loading_state(status)
                
            elif status == 'ChunkingFailed':
                self._show_chunking_failed()
                
            elif status == 'NotRequired':
                if is_duplicate and original_doc_id:
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            f"### 📄 Tài liệu này không yêu cầu chia chunks\nĐây là bản sao của tài liệu gốc: {original_doc_id}",
                            styles={
                                'color': '#4A5568',
                                'text-align': 'center',
                                'padding': '20px',
                                'background': '#F0F5FF',
                                'border-radius': '8px',
                                'margin': '20px 0',
                                'font-size': '16px',
                                'border': '1px solid #BEE3F8'
                            }
                        )
                    )
                    
                    try:
                        original_chunks = self.chroma_manager.get_chunks_by_document_id(original_doc_id)
                        if original_chunks:
                            for chunk in original_chunks:
                                self._display_chunk(chunk, original_doc_id)
                            
                            self.show_info_message(f"Đã tải {len(original_chunks)} chunks từ tài liệu gốc")
                        else:
                            self.chunks_container.append(
                                pn.pane.Markdown(
                                    f"### Không tìm thấy chunks từ tài liệu gốc {original_doc_id}",
                                    styles={
                                        'color': '#DD6B20', 
                                        'text-align': 'center',
                                        'padding': '15px',
                                        'background': '#FFFAF0',
                                        'border-radius': '8px',
                                        'margin-top': '15px'
                                    }
                                )
                            )
                    except Exception as e:
                        self.chunks_container.append(
                            pn.pane.Markdown(
                                f"### Lỗi khi tải chunks từ tài liệu gốc\n{str(e)}",
                                styles={
                                    'color': '#E53E3E',
                                    'text-align': 'center',
                                    'padding': '15px',
                                    'background': '#FFF5F5',
                                    'border-radius': '8px',
                                    'margin-top': '15px'
                                }
                            )
                        )
                elif duplicate_group_id:
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            f"### 📄 Tài liệu này không yêu cầu chia chunks\nTài liệu này thuộc nhóm trùng lặp: {duplicate_group_id}",
                            styles={
                                'color': '#4A5568',
                                'text-align': 'center',
                                'padding': '20px',
                                'background': '#F0F5FF',
                                'border-radius': '8px',
                                'margin': '20px 0',
                                'font-size': '16px',
                                'border': '1px solid #BEE3F8'
                            }
                        )
                    )
                    
                    try:
                        group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                        chunked_docs = [d for d in group_docs if d.get('chunk_status') == 'Chunked']
                        
                        if chunked_docs:
                            chunked_doc_id = chunked_docs[0]['id']
                            original_chunks = self.chroma_manager.get_chunks_by_document_id(chunked_doc_id)
                            if original_chunks:
                                self.chunks_container.append(
                                    pn.pane.Markdown(
                                        f"### Chunks từ tài liệu trong nhóm: {chunked_doc_id}",
                                        styles={
                                            'color': '#3182CE',
                                            'text-align': 'center',
                                            'padding': '15px',
                                            'background': '#EBF8FF',
                                            'border-radius': '8px',
                                            'margin': '10px 0'
                                        }
                                    )
                                )
                                
                                for chunk in original_chunks:
                                    self._display_chunk(chunk, chunked_doc_id)
                                
                                self.show_info_message(f"Đã tải {len(original_chunks)} chunks từ tài liệu trong nhóm")
                        else:
                            self.chunks_container.append(
                                pn.pane.Markdown(
                                    "### Không tìm thấy tài liệu đã chia chunks trong nhóm",
                                    styles={
                                        'color': '#DD6B20',
                                        'text-align': 'center',
                                        'padding': '15px',
                                        'background': '#FFFAF0',
                                        'border-radius': '8px',
                                        'margin-top': '15px'
                                    }
                                )
                            )
                    except Exception as e:
                        self.chunks_container.append(
                            pn.pane.Markdown(
                                f"### Lỗi khi tìm tài liệu trong nhóm\n{str(e)}",
                                styles={
                                    'color': '#E53E3E',
                                    'text-align': 'center',
                                    'padding': '15px',
                                    'background': '#FFF5F5',
                                    'border-radius': '8px',
                                    'margin-top': '15px'
                                }
                            )
                        )
                else:
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            "### 📄 Tài liệu này không yêu cầu chia chunks\nTài liệu này được đánh dấu không cần chia chunks.",
                            styles={
                                'color': '#4A5568',
                                'text-align': 'center',
                                'padding': '20px',
                                'background': '#F0F5FF',
                                'border-radius': '8px',
                                'margin': '20px 0',
                                'font-size': '16px',
                                'border': '1px solid #BEE3F8'
                            }
                        )
                    )
                
            elif status == 'Failed':
                self.chunks_container.append(
                    pn.pane.Markdown(
                        "### ❌ Đã xảy ra lỗi khi xử lý tài liệu này\nKhông thể tạo chunks. Vui lòng liên hệ quản trị viên.",
                        styles={
                            'color': '#E53E3E',
                            'text-align': 'center',
                            'padding': '20px',
                            'background': '#FFF5F5',
                            'border-radius': '8px',
                            'margin': '20px 0',
                            'font-size': '16px',
                            'border': '1px solid #FEB2B2'
                        }
                    )
                )
                
                if document and document.get('error_message'):
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            f"**Chi tiết lỗi:**\n{document.get('error_message')}",
                            styles={
                                'color': '#E53E3E',
                                'background': '#FFF5F5',
                                'padding': '15px',
                                'border-radius': '8px',
                                'margin': '10px 0',
                                'font-size': '14px'
                            }
                        )
                    )
                
                reprocess_button = pn.widgets.Button(
                    name="Xử lý lại tài liệu",
                    button_type="primary",
                    width=200
                )
                
                def reprocess_document(event):
                    try:
                        pass
                    except Exception as e:
                        self.show_error_message(f"Lỗi khi xử lý lại tài liệu: {str(e)}")
                
                reprocess_button.on_click(reprocess_document)
                self.chunks_container.append(pn.Row(reprocess_button, align='center'))
                
            else:
                if document:
                    self._handle_referenced_chunks(document)
                else:
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            "### ❓ Không thể xác định trạng thái chunk\nKhông thể tải thông tin chunk cho tài liệu này.",
                            styles={
                                'color': '#805AD5',
                                'text-align': 'center',
                                'padding': '20px',
                                'background': '#FAF5FF',
                                'border-radius': '8px',
                                'margin': '20px 0',
                                'font-size': '16px',
                                'border': '1px solid #D6BCFA'
                            }
                        )
                    )
                    
                    self.show_error_message(f"Không thể tải chunks: Trạng thái không xác định ({status})")
                    
        except Exception as e:
            logger.error(f"Error handling non-chunked status: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_error_message(f"Lỗi xử lý chunks: {str(e)}")
    
    def _display_chunks_with_pagination(self, chunks, doc_id, page=1, page_size=10):
        try:
            self.chunks_container.clear()
            
            total_chunks = len(chunks)
            
            self.chunks_container.append(
                pn.pane.Markdown(
                    f"**Tổng số chunks:** {total_chunks}",
                    styles={
                        'color': '#3182CE',
                        'background': '#EBF8FF',
                        'padding': '10px',
                        'border-radius': '4px',
                        'margin': '5px 0 15px 0',
                        'text-align': 'center'
                    }
                )
            )
            
            for chunk in chunks:
                self._display_chunk(chunk, doc_id)
            
        except Exception as e:
            logger.error(f"Error displaying chunks: {str(e)}")
            self.show_error_message(f"Lỗi hiển thị chunks: {str(e)}")
        
    
    def _handle_referenced_chunks(self, document):
        """
        Handle chunks for references/duplicates
        """
        try:
            self.chunks_container.clear()
            self.chunks_container.append(
                pn.Column(
                    pn.indicators.LoadingSpinner(value=True, size=50),
                    pn.pane.Markdown(
                        "### Đang phân tích tài liệu...",
                        styles={
                            'color': '#4A5568',
                            'text-align': 'center',
                            'padding': '20px'
                        }
                    ),
                    align='center'
                )
            )
            
            doc_id = document.get('id')
            original_doc = document.get('original_chunked_doc')
            duplicate_group_id = document.get('duplicate_group_id')
            is_duplicate = document.get('is_duplicate', False)
            chunk_status = document.get('chunk_status')
            
            logger.info(f"Phân tích tài liệu {doc_id}:")
            logger.info(f"- original_chunked_doc: {original_doc}")
            logger.info(f"- duplicate_group_id: {duplicate_group_id}")
            logger.info(f"- is_duplicate: {is_duplicate}")
            logger.info(f"- chunk_status: {chunk_status}")
            
            if is_duplicate and original_doc and original_doc != doc_id:
                self.chunks_container.clear()
                self.chunks_container.append(
                    pn.pane.Markdown(
                        "### Tài liệu này là bản trùng lắp\n" +
                        f"Chunks được tham chiếu từ tài liệu gốc: {original_doc}",
                        styles={
                            'color': '#4A5568',
                            'text-align': 'center',
                            'padding': '20px',
                            'background': '#F7FAFC',
                            'border-radius': '8px',
                            'margin': '20px 0',
                            'font-size': '16px',
                            'border': '1px solid #E2E8F0'
                        }
                    )
                )
                
                try:
                    logger.info(f"Load chunks from original document {original_doc}")
                    original_chunks = self.chroma_manager.get_chunks_by_document_id(original_doc)
                    
                    if original_chunks:
                        for chunk in original_chunks:
                            self._display_chunk(chunk, original_doc)
                        self.show_info_message(f"Loaded {len(original_chunks)} chunks from original document")
                    else:
                        logger.warning(f"No chunks found from original document {original_doc}")
                        self.chunks_container.append(
                            pn.pane.Markdown(
                                "### Không tìm thấy chunks từ tài liệu gốc",
                                styles={
                                    'color': '#E53E3E',
                                    'text-align': 'center',
                                    'padding': '15px',
                                    'background': '#FFF5F5',
                                    'border-radius': '8px',
                                    'margin-top': '20px'
                                }
                            )
                        )
                except Exception as e:
                    logger.error(f"Error loading chunks from original document: {str(e)}")
                    logger.error(traceback.format_exc())
                    self.show_error_message(f"Lỗi khi tải chunks từ tài liệu gốc: {str(e)}")
                
                return
            
            if original_doc == doc_id:
                logger.warning(f"Document references itself as original: {doc_id}")
                original_doc = None
                
            if not original_doc and duplicate_group_id:
                group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                if group_docs:
                    chunked_docs = [d for d in group_docs if d.get('chunk_status') == 'Chunked']
                    if chunked_docs:
                        chunks_source = chunked_docs[0]['id']
                        if chunks_source != doc_id:
                            self.chunks_container.clear()
                            self.chunks_container.append(
                                pn.pane.Markdown(
                                    "### Tài liệu này là bản trùng lắp\n" +
                                    f"Chunks được tham chiếu từ tài liệu: {chunks_source}",
                                    styles={
                                        'color': '#4A5568',
                                        'text-align': 'center',
                                        'padding': '20px',
                                        'background': '#F7FAFC',
                                        'border-radius': '8px',
                                        'margin': '20px 0',
                                        'font-size': '16px',
                                        'border': '1px solid #E2E8F0'
                                    }
                                )
                            )
                            
                            try:
                                group_chunks = self.chroma_manager.get_chunks_by_document_id(chunks_source)
                                if group_chunks:
                                    for chunk in group_chunks:
                                        self._display_chunk(chunk, chunks_source)
                                    self.show_info_message(f"Loaded {len(group_chunks)} chunks from document {chunks_source}")
                                    return
                            except Exception as group_error:
                                logger.error(f"Error loading chunks from group document: {str(group_error)}")
            
            try:
                current_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
                if current_chunks:
                    self.chunks_container.clear()
                    for chunk in current_chunks:
                        self._display_chunk(chunk, doc_id)
                    self.show_info_message(f"Loaded {len(current_chunks)} chunks")
                    return
                else:
                    self.chunks_container.clear()
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            "### Không tìm thấy chunks cho tài liệu này",
                            styles={
                                'color': '#E53E3E',
                                'text-align': 'center',
                                'padding': '15px',
                                'background': '#FFF5F5',
                                'border-radius': '8px'
                            }
                        )
                    )
                    
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            f"#### Thông tin tài liệu:\n"
                            f"- ID: {doc_id}\n"
                            f"- Trạng thái chunks: {chunk_status}\n"
                            f"- Thuộc nhóm: {duplicate_group_id if duplicate_group_id else 'Không'}\n",
                            styles={
                                'color': '#4A5568',
                                'background': '#EDF2F7',
                                'padding': '15px',
                                'border-radius': '8px',
                                'margin-top': '15px'
                            }
                        )
                    )
                    self.show_error_message("Không tìm thấy chunks cho tài liệu này")
            except Exception as e:
                logger.error(f"Error loading chunks: {str(e)}")
                logger.error(traceback.format_exc())
                self.show_error_message(f"Lỗi khi tải chunks: {str(e)}")
                    
        except Exception as e:
            logger.error(f"Error handling referenced chunks: {str(e)}")
            logger.error(traceback.format_exc())
            
            self.chunks_container.clear()
            self.chunks_container.append(
                pn.pane.Markdown(
                    "### Đã xảy ra lỗi khi xử lý chunks",
                    styles={
                        'color': '#E53E3E',
                        'text-align': 'center',
                        'padding': '15px',
                        'background': '#FFF5F5',
                        'border-radius': '8px',
                        'margin-top': '20px'
                    }
                )
            )
            
            self._is_loading_chunks = False
     
    def _show_loading_state(self, status):
        """Display appropriate loading status"""
        message = {
            'Pending': "### Hệ thống đang chuẩn bị chia chunks cho tài liệu này",
            'Chunking': "### Đang trong quá trình chia chunks",
            'Loading': "### Đang tải chunks..."
        }.get(status, "### Đang xử lý...")
            
        self.chunks_container.clear()
        spinner = pn.indicators.LoadingSpinner(value=True, size=50)
    
        self.chunks_container.append(
            pn.Column(
                spinner,
                pn.pane.Markdown(
                    f"{message}\nVui lòng chờ trong giây lát...",
                    styles={
                        'color': '#4A5568',
                        'text-align': 'center',
                        'padding': '20px',
                        'background': '#EDF2F7',
                        'border-radius': '8px', 
                        'margin': '20px 0',
                        'font-size': '16px'
                    }
                ),
                align='center'
            )
        )
        
    def _show_chunking_failed(self):
        """Show failed chunk split status"""
        self.chunks_container.clear()
        self.chunks_container.append(
            pn.pane.Markdown(
                "### ❌ Quá trình chia chunks thất bại\nVui lòng thử lại sau hoặc liên hệ admin.",
                styles={
                    'color': '#E53E3E',
                    'text-align': 'center',
                    'padding': '20px',
                    'background': '#FFF5F5',
                    'border-radius': '8px',
                    'margin': '20px 0',
                    'font-size': '16px',
                    'border': '1px solid #FEB2B2'
                }
            )
        )

    def check_reanalysis_needed(self):
        """
        Check if current document needs conflict reanalysis and show notification if needed
        """
        try:
            if not self.current_doc_id:
                return
            
            current_time = time.time()
            last_check = getattr(self, '_last_reanalysis_check', 0)
            if current_time - last_check < 10:  # Check every 10 seconds at most
                return
                
            self._last_reanalysis_check = current_time
            
            document = self.data_manager.get_document_by_id(self.current_doc_id)
            if not document:
                return
                
            needs_reanalysis = document.get('needs_conflict_reanalysis', False)
            if needs_reanalysis:
                if hasattr(self, 'tabs') and self.tabs.active == 3:
                    if not hasattr(self, '_reanalysis_notice_shown') or not self._reanalysis_notice_shown:
                        self._reanalysis_notice_shown = True
                        
                        if hasattr(self, 'conflicts_container'):
                            reanalysis_notice = pn.pane.Markdown(
                                "### Mâu thuẫn có thể đã cũ\nChunk đã được thay đổi. Hãy phân tích lại để cập nhật thông tin mâu thuẫn.",
                                styles={
                                    'color': '#c05621',
                                    'background': '#fffaf0',
                                    'padding': '10px',
                                    'border-radius': '4px',
                                    'border': '1px solid #fbd38d',
                                    'margin': '10px 0'
                                }
                            )
                            
                            notice_row = pn.Row(
                                reanalysis_notice,
                                name='reanalysis_notice',
                                sizing_mode='stretch_width'
                            )
                            
                            if not any(hasattr(obj, 'name') and obj.name == 'reanalysis_notice' 
                                    for obj in self.conflicts_container):
                                self.conflicts_container.insert(0, notice_row)
                
                elif not hasattr(self, '_reanalysis_notification_shown') or not self._reanalysis_notification_shown:
                    self._reanalysis_notification_shown = True
                    self.show_notification(
                        "Mâu thuẫn cần được phân tích lại.",
                        alert_type="warning",
                        duration=5000
                    )
                    
                    def reset_notification_flag():
                        self._reanalysis_notification_shown = False
                        
                    if not hasattr(self, '_reset_notification_timer'):
                        import threading
                        self._reset_notification_timer = threading.Timer(30.0, reset_notification_flag)
                        self._reset_notification_timer.daemon = True
                        self._reset_notification_timer.start()
                    else:
                        try:
                            self._reset_notification_timer.cancel()
                        except:
                            pass
                        
                        import threading
                        self._reset_notification_timer = threading.Timer(30.0, reset_notification_flag)
                        self._reset_notification_timer.daemon = True
                        self._reset_notification_timer.start()
                    
            else:
                self._reanalysis_notice_shown = False
                self._reanalysis_notification_shown = False
                
                if hasattr(self, 'conflicts_container'):
                    for i, obj in enumerate(self.conflicts_container):
                        if hasattr(obj, 'name') and obj.name == 'reanalysis_notice':
                            self.conflicts_container.pop(i)
                            break
                        
        except Exception as e:
            logger.error(f"Error checking if reanalysis is needed: {str(e)}")
    
    def _display_chunk(self, chunk, source_doc_id):
        try:
            chunk_id = chunk['id']
            topic = chunk.get('document_topic', '').strip()
            original_text = chunk.get('original_text', '').strip()
            chunk_metadata = chunk.get('metadata', {})
            is_enabled = chunk_metadata.get('is_enabled', True)
            
            is_duplicate = source_doc_id and source_doc_id != chunk_id.split('_paragraph_')[0]

            notification = pn.pane.Alert(
                "",
                alert_type="success",
                visible=False,
                margin=(0, 0, 10, 0)
            )

            def get_banner_styles(enabled):
                """Create styles for banner based on status"""
                if is_duplicate:
                    return {
                        'background': '#e9d8fd', 
                        'color': '#553c9a',
                        'padding': '10px 15px',
                        'border-radius': '4px',
                        'font-size': '14px',
                        'font-weight': 'bold',
                        'margin': '-20px -20px 15px -20px',
                        'transition': 'all 0.3s ease'
                    }
                elif enabled:
                    return {
                        'background': '#d4ffd4',
                        'color': '#0a3622',
                        'padding': '10px 15px',
                        'border-radius': '4px',
                        'font-size': '14px',
                        'font-weight': 'bold',
                        'margin': '-20px -20px 15px -20px',
                        'transition': 'all 0.3s ease'
                    }
                else:
                    return {
                        'background': '#fff3cd',
                        'color': '#856404',
                        'padding': '10px 15px',
                        'border-radius': '4px',
                        'font-size': '14px',
                        'font-weight': 'bold',
                        'margin': '-20px -20px 15px -20px',
                        'transition': 'all 0.3s ease'
                    }

            def show_notification(message, alert_type="info", duration=5000):
                try:
                    if pn.state.notifications is not None:
                        if alert_type == "success":
                            pn.state.notifications.success(message, duration=duration)
                        elif alert_type == "error":
                            pn.state.notifications.error(message, duration=duration)
                        elif alert_type == "warning":
                            pn.state.notifications.warning(message, duration=duration)
                        else:
                            pn.state.notifications.info(message, duration=duration)
                    else:
                        notification.alert_type = alert_type
                        notification.object = message
                        notification.visible = True
                except Exception as e:
                    logger.error(f"Error displaying notification: {str(e)}")
                    
     
            def handle_checkbox_change(event):
                """Handle chunk state change events with automatic conflict analysis"""
                try:
                    current_time = time.time() * 1000
                    last_update = getattr(self, '_last_chunk_update_time', 0)
                    if current_time - last_update < 1000:
                        event.obj.value = not event.new
                        show_notification(
                            'Please wait a moment before trying again',
                            alert_type="warning",
                            duration=1000
                        )
                        return

                    self._last_chunk_update_time = current_time
                    new_state = event.new
                    loading.visible = True
                    
                    if hasattr(self, '_is_updating') and self._is_updating:
                        return
                        
                    self._is_updating = True

                    try:
                        doc_id = chunk_id.split('_paragraph_')[0] if '_paragraph_' in chunk_id else None
                        
                        document = None
                        duplicate_group_id = None
                        if doc_id:
                            document = self.data_manager.get_document_by_id(doc_id)
                            duplicate_group_id = document.get('duplicate_group_id') if document else None
                        
                        related_docs = [doc_id] if doc_id else []
                        if duplicate_group_id:
                            group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                            if group_docs:
                                related_docs = [d['id'] for d in group_docs]
                        
                        result = self.chroma_manager.update_chunk_metadata(
                            chunk_id=chunk_id,
                            metadata={'is_enabled': new_state}
                        )

                        if result is True:
                            status.object = 'Chunk enabled' if new_state else 'Chunk disabled'
                            status.styles = get_banner_styles(new_state)
                            
                            card.styles = get_card_styles(new_state)
                            
                            cache_key = f"{chunk_id.split('_paragraph_')[0]}_chunks"
                            if cache_key in self._chunks_cache:
                                for cached_chunk in self._chunks_cache[cache_key]:
                                    if cached_chunk['id'] == chunk_id:
                                        if 'metadata' not in cached_chunk:
                                            cached_chunk['metadata'] = {}
                                        cached_chunk['metadata']['is_enabled'] = new_state
                                        break
                            
                            show_notification(
                                f"Chunk {'enabled' if new_state else 'disabled'} thành công. Đang bắt đầu phân tích mâu thuẫn...",
                                alert_type="success",
                                duration=3000
                            )
                            
                            def analyze_conflicts_background():
                                try:
                                    if not hasattr(self, 'conflict_manager'):
                                        self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                                    
                                    if doc_id:
                                        try:
                                            logger.info(f"Auto-analyzing conflicts for document {doc_id} after chunk state change")
                                            self.conflict_manager.analyze_document(doc_id)
                                            
                                            if self.current_doc_id == doc_id:
                                                if hasattr(self, 'tabs') and self.tabs.active == 3:
                                                    def update_conflicts_ui():
                                                        try:
                                                            self.load_conflicts_data(doc_id)
                                                            show_notification(
                                                                "Conflict analysis completed", 
                                                                alert_type="success"
                                                            )
                                                        except Exception as ui_error:
                                                            error_msg = str(ui_error).replace('%', '%%')
                                                            logger.error(f"Error updating conflicts UI: {error_msg}")
                                                    
                                                    import threading
                                                    ui_thread = threading.Thread(target=update_conflicts_ui)
                                                    ui_thread.daemon = True
                                                    ui_thread.start()
                                        except Exception as analyze_error:
                                            error_msg = str(analyze_error).replace('%', '%%')
                                            logger.error(f"Error analyzing document {doc_id}: {error_msg}")
                                    
                                    if duplicate_group_id:
                                        for related_id in related_docs:
                                            if related_id != doc_id:
                                                try:
                                                    logger.info(f"Auto-analyzing related document {related_id}")
                                                    self.conflict_manager.analyze_document(related_id)
                                                except Exception as related_error:
                                                    error_msg = str(related_error).replace('%', '%%')
                                                    logger.error(f"Error analyzing related document {related_id}: {error_msg}")
                                        
                                        try:
                                            self.conflict_manager.sync_group_conflicts_by_group(duplicate_group_id)
                                        except Exception as sync_error:
                                            logger.error(f"Error syncing group conflicts: {str(sync_error)}")
                                
                                except Exception as background_error:
                                    logger.error(f"Error in conflict analysis background thread: {str(background_error)}")
                                    logger.error(traceback.format_exc())
                            
                            import threading
                            analysis_thread = threading.Thread(target=analyze_conflicts_background)
                            analysis_thread.daemon = True
                            analysis_thread.start()
                            
                        else:
                            event.obj.value = not new_state
                            show_notification(
                                "Cannot update chunk status",
                                alert_type="error",
                                duration=5000
                            )
                    except Exception as e:
                        event.obj.value = not new_state
                        logger.error(f"Error updating chunk {chunk_id}: {str(e)}")
                        show_notification(
                            "Error updating chunk",
                            alert_type="error",
                            duration=5000
                        )
                    finally:
                        loading.visible = False
                        self._is_updating = False

                except Exception as e:
                    logger.error(f"Error processing chunk event {chunk_id}: {str(e)}")
                    loading.visible = False
                    self._is_updating = False
            
            def get_card_styles(enabled):
                """Get styles for card container"""
                base_styles = {
                    'background': '#ffffff',
                    'border': '1px solid #e2e8f0',
                    'border-radius': '8px',
                    'padding': '20px',
                    'margin': '10px 0',
                    'transition': 'all 0.3s ease'
                }
                
                if is_duplicate:
                    base_styles.update({
                        'border-left': '4px solid #9f7aea',
                        'background': '#faf5ff'
                    })
                if not enabled:
                    base_styles.update({
                        'opacity': '0.6',
                        'background': '#f7f7f7'
                    })
                return base_styles

            status_text = ('Duplicate chunk - ' if is_duplicate else '') + ('Chunk enabled' if is_enabled else 'Chunk disabled')
            status = pn.pane.Markdown(
                status_text,
                styles=get_banner_styles(is_enabled)
            )

            checkbox = pn.widgets.Checkbox(
                name='Sử dụng chunk này',
                value=is_enabled,
                width=150
            )
            
            loading = pn.indicators.LoadingSpinner(
                value=False,
                size=20,
                visible=False
            )

            checkbox.param.watch(handle_checkbox_change, 'value')

            card = pn.Column(
                status,
                pn.Row(
                    pn.pane.Markdown(
                        f"Chunk {chunk_id}" + (f" (from original document: {source_doc_id})" if is_duplicate else ""),
                        styles={
                            'font-weight': 'bold',
                            'font-size': '14px',
                            'margin': '0',
                            'flex': '1'
                        }
                    ),
                    pn.Row(
                        checkbox,
                        loading,
                        align='center'
                    ),
                    sizing_mode='stretch_width',
                    align='center'
                ),
                pn.pane.Markdown(
                    '\n'.join([
                        *(["**Topic:** " + topic] if topic else []),
                        "\n**Original Text:**",
                        original_text
                    ]),
                    styles={
                        'background': '#f8fafc',
                        'padding': '15px',
                        'border-radius': '6px',
                        'font-size': '13px',
                        'line-height': '1.6',
                        'border-left': '4px solid #4299e1',
                        'margin-top': '10px'
                    }
                ),
                styles=get_card_styles(is_enabled),
                sizing_mode='stretch_width'
            )

            self.chunks_container.append(card)

        except Exception as e:
            logger.error(f"Error displaying chunk {chunk.get('id', 'unknown')}: {str(e)}")
            logger.error(traceback.format_exc())
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
    def load_similar_documents(self, doc_id):
        """
        Load and display documents that are similar to the selected document.
        
        This method performs the following steps:
        - Clears the current container content
        - Loads the original document information using the ID
        - Checks for similar document groups
        - Creates and displays a card for each similar document
        - Shows total count of similar documents
        
        Args:
            doc_id (str): The ID of the document to find similar documents for
            
        Raises:
            Exception: When there is an error loading the similar documents data
            
        Returns:
            None: Method updates the UI directly instead of returning values
        """

        try:
            self.similar_docs_container.clear()
            self.clear_messages()

            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                self.show_error_message("Không tìm thấy thông tin tài liệu")
                return

            duplicate_group_id = document.get('duplicate_group_id')
            if not duplicate_group_id:
                self.similar_docs_container.append(
                    pn.pane.Markdown(
                        "**Không có tài liệu tương đồng với tài liệu này**",
                        styles={
                            'color': '#666',
                            'font-size': '14px',
                            'margin-top': '10px',
                            'text-align': 'center',
                            'background': '#f9fafb',
                            'padding': '20px',
                            'border-radius': '8px',
                            'border': '1px solid #e5e7eb',
                            'box-shadow': '0 1px 2px rgba(0, 0, 0, 0.05)'
                        }
                    )
                )
                return

            group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
            if not group_docs:
                self.similar_docs_container.append(
                    pn.pane.Markdown(
                        "**Không có tài liệu tương đồng với tài liệu này**",
                        styles={
                            'color': '#666',
                            'font-size': '14px',
                            'margin-top': '10px',
                            'text-align': 'center',
                            'background': '#f9fafb',
                            'padding': '20px',
                            'border-radius': '8px',
                            'border': '1px solid #e5e7eb',
                            'box-shadow': '0 1px 2px rgba(0, 0, 0, 0.05)'
                        }
                    )
                )
                return

            similar_docs = []
            for doc in group_docs:
                if doc['id'] != doc_id:
                    similar_docs.append(doc)

            if not similar_docs:
                self.similar_docs_container.append(
                    pn.pane.Markdown(
                        "**Không có tài liệu tương đồng với tài liệu này**",
                        styles={
                            'color': '#666',
                            'font-size': '14px',
                            'margin-top': '10px',
                            'text-align': 'center',
                            'background': '#f9fafb',
                            'padding': '20px',
                            'border-radius': '8px',
                            'border': '1px solid #e5e7eb',
                            'box-shadow': '0 1px 2px rgba(0, 0, 0, 0.05)'
                        }
                    )
                )
                return

            self.similar_docs_container.append(
                pn.pane.Markdown(
                    f"ℹ️ Tổng số tài liệu tương đồng: {len(similar_docs)}", 
                    styles={
                        'color': '#2c5282', 
                        'font-size': '14px', 
                        'margin': '10px 0',
                        'padding': '10px',
                        'background': '#ebf8ff',
                        'border-radius': '4px'
                    }
                )
            )

            for doc in similar_docs:
                similarity_score = doc.get('similarity_score', 1)
                similarity_percent = f"{int(similarity_score * 100)}%" if similarity_score is not None else "100%"
                
                content = pn.pane.Markdown(f"""
                **ID:** {doc['id']}
                
                **Nội dung:** {self.get_content_preview(doc['content'])}
                
                **Ngày tạo:** {format_date(doc['created_date'])}
                
                **Người gửi:** {doc.get('sender', '')}
                
                **Trạng thái:** {self.format_approval_status(doc['approval_status'])}
                
                **Độ tương đồng:** {similarity_percent}
                """, styles={'font-size': '13px'})

                card = pn.Card(
                    content,
                    styles={
                        'background': '#ffffff',
                        'border': '1px solid #e2e8f0',
                        'border-radius': '8px',
                        'box-shadow': '0 1px 3px 0 rgba(0, 0, 0, 0.1)',
                        'padding': '20px',
                        'margin': '10px 0'
                    }
                )
                self.similar_docs_container.append(card)

        except Exception as e:
            logger.error(f"Error loading similar documents: {str(e)}")
            self.show_error_message(f"Lỗi khi tải tài liệu tương đồng: {str(e)}")

            self.similar_docs_container.append(
                pn.pane.Markdown(
                    "❌ **Lỗi khi tải tài liệu tương đồng**",
                    styles={
                        'color': '#e53e3e',
                        'font-size': '14px',
                        'margin': '10px 0',
                        'padding': '10px',
                        'background': '#fff5f5',
                        'border-radius': '4px',
                        'text-align': 'center'
                    }
                )
            )
            
    def load_conflicts_data(self, doc_id):
        try:
            if hasattr(self, '_is_loading_conflicts') and self._is_loading_conflicts:
                return

            self._is_loading_conflicts = True
            
            cache_key = f"{doc_id}_conflicts"
            cached_conflicts = self._get_from_cache(cache_key)
            
            if cached_conflicts:
                self._display_cached_conflicts(cached_conflicts, doc_id)
                
                try:
                    if hasattr(pn.state, 'add_timeout_callback'):
                        pn.state.add_timeout_callback(
                            lambda: self._load_fresh_conflicts_data(doc_id, cache_key),
                            500
                        )
                    elif hasattr(pn, 'callbacks') and hasattr(pn.callbacks, 'timeout'):
                        pn.callbacks.timeout(500, lambda: self._load_fresh_conflicts_data(doc_id, cache_key))
                    else:
                        import threading
                        thread = threading.Thread(
                            target=self._load_fresh_conflicts_data,
                            args=(doc_id, cache_key),
                            daemon=True
                        )
                        thread.start()
                except Exception as async_error:
                    logger.warning(f"Error scheduling fresh conflicts loading: {str(async_error)}")
                    
                    self._load_fresh_conflicts_data(doc_id, cache_key)
                return
                    
            if hasattr(self, 'conflicts_container'):
                self.conflicts_container.clear()
                self.conflicts_container.append(
                    pn.Column(
                        pn.indicators.LoadingSpinner(value=True, size=40),
                        pn.pane.Markdown(
                            "### Đang tải thông tin mâu thuẫn...",
                            styles={
                                'color': '#4A5568',
                                'text-align': 'center',
                                'padding': '20px',
                                'background': '#EDF2F7',
                                'border-radius': '8px',
                                'margin': '20px 0'
                            }
                        ),
                        align='center'
                    )
                )
            
            try:
                if hasattr(pn.state, 'add_timeout_callback'):
                    pn.state.add_timeout_callback(
                        lambda: self._load_fresh_conflicts_data(doc_id, cache_key),
                        100
                    )
                elif hasattr(pn, 'callbacks') and hasattr(pn.callbacks, 'timeout'):
                    pn.callbacks.timeout(100, lambda: self._load_fresh_conflicts_data(doc_id, cache_key))
                else:
                    import threading
                    thread = threading.Thread(
                        target=self._load_fresh_conflicts_data,
                        args=(doc_id, cache_key),
                        daemon=True
                    )
                    thread.start()
            except Exception as async_error:
                logger.warning(f"Error scheduling conflicts loading: {str(async_error)}")
           
                self._load_fresh_conflicts_data(doc_id, cache_key)
                    
        except Exception as e:
            logger.error(f"Error in load_conflicts_data: {str(e)}")
            logger.error(traceback.format_exc())
            self._is_loading_conflicts = False
            self.show_error_message(f"Lỗi khi tải thông tin mâu thuẫn: {str(e)}")
         
    def _load_fresh_conflicts_data(self, doc_id, cache_key):
        """
        Tải dữ liệu mâu thuẫn mới và cập nhật cache
        """
        try:
            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                if hasattr(self, 'conflicts_container'):
                    self.conflicts_container.clear()
                    self.conflicts_container.append(
                        pn.pane.Markdown(
                            "### Không tìm thấy thông tin tài liệu",
                            styles={
                                'color': '#E53E3E',
                                'text-align': 'center',
                                'padding': '15px',
                                'background': '#FFF5F5',
                                'border-radius': '8px',
                                'margin': '10px 0'
                            }
                        )
                    )
                self._is_loading_conflicts = False
                return

            conflict_analysis_status = document.get('conflict_analysis_status', 'NotAnalyzed')
            conflict_status = document.get('conflict_status', 'No Conflict')
            
            if conflict_analysis_status == 'Analyzing' or conflict_status == 'Analyzing':
                if hasattr(self, 'conflicts_container'):
                    self.conflicts_container.clear()
                    self.conflicts_container.append(
                        pn.Column(
                            pn.indicators.LoadingSpinner(value=True, size=40),
                            pn.pane.Markdown(
                                "### Đang phân tích mâu thuẫn...\nVui lòng chờ trong giây lát...",
                                styles={
                                    'color': '#4A5568',
                                    'text-align': 'center',
                                    'padding': '20px',
                                    'background': '#EDF2F7',
                                    'border-radius': '8px',
                                    'margin': '20px 0'
                                }
                            ),
                            align='center'
                        )
                    )
                    
                self._last_conflict_reload = time.time()
                self._is_loading_conflicts = False
                return
                    
            conflict_info = document.get('conflict_info')
            has_conflicts = document.get('has_conflicts', False)
            
            conflicts_data = {
                'content_conflicts': [],
                'internal_conflicts': [],
                'external_conflicts': [],
                'last_updated': datetime.now().isoformat(),
                'analysis_status': conflict_analysis_status,
                'has_conflicts': has_conflicts,
                'conflict_status': conflict_status
            }
            
            if conflict_info:
                if isinstance(conflict_info, str):
                    try:
                        conflict_info = json.loads(conflict_info)
                    except json.JSONDecodeError:
                        conflict_info = {}
                
                if isinstance(conflict_info, dict):
                    content_conflicts = conflict_info.get('content_conflicts', [])
                    internal_conflicts = conflict_info.get('internal_conflicts', [])
                    external_conflicts = conflict_info.get('external_conflicts', [])
                    
                    
                    if not isinstance(content_conflicts, list):
                        content_conflicts = []
                    if not isinstance(internal_conflicts, list):
                        internal_conflicts = []
                    if not isinstance(external_conflicts, list):
                        external_conflicts = []
                    
                    logger.info(f"Loaded conflicts data: content={len(content_conflicts)}, "
                            f"internal={len(internal_conflicts)}, external={len(external_conflicts)}")
                    
                    total_conflicts = {
                        'content': len(content_conflicts),
                        'internal': len(internal_conflicts),
                        'external': len(external_conflicts),
                        'total': len(content_conflicts) + len(internal_conflicts) + len(external_conflicts)
                    }
                    
                    conflicts_data['content_conflicts'] = content_conflicts[:20]
                    conflicts_data['internal_conflicts'] = internal_conflicts[:20]
                    conflicts_data['external_conflicts'] = external_conflicts[:20]
                    conflicts_data['total_conflicts'] = total_conflicts
                
                self._update_cache(cache_key, conflicts_data, priority=3)
                
                self._display_cached_conflicts(conflicts_data, doc_id)
        
        except Exception as e:
            logger.error(f"Error loading fresh conflicts data: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_error_message(f"Lỗi khi tải thông tin mâu thuẫn mới: {str(e)}")
        finally:
            self._is_loading_conflicts = False
    
    def get_content_preview(self, content):
        """
        Format a content preview with a specified maximum length.

        Args:
            content (str): The content to be previewed.
            max_length (int, optional): The maximum allowed length of the content preview. 
                                        Defaults to 100 if not specified.

        Returns:
            str: A string representing the preview of the content, either truncated with 
                an ellipsis or the content itself if it's within the max length.
        """
        if not content:
            return ""
        
        return content[:max_length] + "..." if len(content) > max_length else content

    def format_approval_status(self, status):
        """
        Format approval status for display.

        Args:
            status (str): The approval status code to be formatted.

        Returns:
            str: The formatted approval status in Vietnamese, or the original status 
                if not found in the predefined list.
        """
        return status

    def get_filtered_data(self, status=None):
        try:
            
            if status and status != "All":
                result = self.data_manager.get_filtered_data(status=status)
            else:
                result = self.data_manager.get_all_documents()
                
            if result is None or len(result) == 0:
                return pd.DataFrame(columns=self.displayed_columns)
                
            return result
                
        except Exception as e:
            logger.error(traceback.format_exc())
            return pd.DataFrame(columns=self.displayed_columns)
    
    def update_table(self, event=None):
        try:
            if getattr(self, '_is_updating', False):
                return
                        
            self._is_updating = True
            
            if hasattr(self, 'loading_indicator'):
                self.loading_indicator.value = True
                        
            status_filter = self.doc_type_selector.value if hasattr(self, 'doc_type_selector') else "All"
            
            cache_key = f"table_data_{status_filter}"
            cached_data = self._get_from_cache(cache_key)
            
            if cached_data is not None:
                logger.info(f"Using cached data for filter: {status_filter}")
                self.all_data = cached_data
            else:
                try:
                    if status_filter != "All":
                        self.all_data = self.data_manager.get_filtered_data(status=status_filter)
                    else:
                        self.all_data = self.data_manager.get_all_documents()
                    
                    self._update_cache(cache_key, self.all_data, priority=4)
                            
                except Exception as db_error:
                    logger.error(f"Error getting data: {str(db_error)}")
                    logger.error(traceback.format_exc())
                    
            if self.current_doc_id and self._check_conflict_update_needed():
                self._update_conflict_for_current_doc()
                    
            try:
                if not hasattr(self, 'data_table'):
                    if hasattr(self, 'loading_indicator'):
                        self.loading_indicator.value = False
                    self._is_updating = False
                    return
                    
                available_columns = [col for col in self.displayed_columns if col in self.all_data.columns]
                
                if hasattr(self, '_last_data_hash'):
                    import hashlib
                    new_hash = hashlib.md5(pd.util.hash_pandas_object(self.all_data[available_columns]).values).hexdigest()
                    
                    if new_hash == self._last_data_hash:
                        logger.info("Data unchanged, skipping table update")
                        self._is_updating = False
                        if hasattr(self, 'loading_indicator'):
                            self.loading_indicator.value = False
                        return
                        
                    self._last_data_hash = new_hash
                else:
                    import hashlib
                    self._last_data_hash = hashlib.md5(pd.util.hash_pandas_object(self.all_data[available_columns]).values).hexdigest()
                
                self._format_initial_data()

                filtered_data = self.all_data[available_columns].copy()
                
                if not hasattr(self, '_last_table_value') or not self._last_table_value.equals(filtered_data):
                    self.data_table.value = filtered_data
                    self._last_table_value = filtered_data.copy()
                    
                    if self.current_doc_id and self.data_table.selection:
                        selected_index = self.data_table.selection[0]
                        if selected_index < len(self.all_data):
                            self.update_detail_view(selected_index)
                        
                else:
                    logger.info("Table value unchanged, no update needed")
                        
            except Exception as ui_error:
                logger.error(f"Error updating table UI: {str(ui_error)}")
                logger.error(traceback.format_exc())
                        
        except Exception as e:
            logger.error(f"Error in update_table: {str(e)}")
            logger.error(traceback.format_exc())
        finally:
            self._is_updating = False
            if hasattr(self, 'loading_indicator'):
                self.loading_indicator.value = False
            self._last_update = datetime.now()
    
    def memoize(func):
        """Decorator để cache kết quả của các hàm nặng"""
        cache = {}
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            if len(args) > 1:
                key = hash(args[1])  
            else:
                key = hash(str(kwargs))
                
            if key not in cache:
                cache[key] = func(*args, **kwargs)
                
                if len(cache) > 100:
                    import random
                    random_key = random.choice(list(cache.keys()))
                    del cache[random_key]
                    
            return cache[key]
        
        return wrapper
    
    @memoize
    def extract_qa_pairs(self, qa_content: str) -> list:
        """
        Extract question and answer pairs from content in both Q/A and Hỏi/Đáp formats.
        
        Args:
            qa_content (str): Raw content containing Q&A pairs
                
        Returns:
            list: List of (question, answer) tuples
        """
        if not qa_content:
            return []

        qa_pairs = []

        normalized_content = (qa_content.replace('FAQs:', '')
                    .replace('**Hỏi:**', 'Q:')
                    .replace('**Đáp:**', 'A:')
                    .replace('Hỏi:', 'Q:')
                    .replace('Đáp:', 'A:')
                    .replace('Câu hỏi:', 'Q:')
                    .replace('Trả lời:', 'A:')
                    .replace('H:', 'Q:')
                    .replace('C:', 'A:'))

        if 'ORIGINAL TEXT:' in normalized_content:
            normalized_content = normalized_content.split('ORIGINAL TEXT:')[0]

        parts = normalized_content.split('Q:')
        for part in parts[1:]:
            if 'A:' in part:
                q, a = part.split('A:', 1)
                q = q.strip() 
                a = a.strip()
                if q and a:
                    qa_pairs.append(('Q: ' + q, 'A: ' + a))

        return qa_pairs
    
    def _update_conflict_for_current_doc(self):
        """Update conflicting information for current document"""
        try:
            if not self.current_doc_id:
                return
                    
            document = self.data_manager.get_document_by_id(self.current_doc_id)
            if not document:
                return
                    
            duplicate_group_id = document.get('duplicate_group_id')
            if not duplicate_group_id:
                return
                    
            if not hasattr(self, 'conflict_manager'):
                self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                    
            sync_info = self.conflict_manager.sync_group_conflicts(self.current_doc_id)
            
            if sync_info and 'status' in sync_info:
                for idx, row in self.all_data.iterrows():
                    if row['id'] == self.current_doc_id:
                        self.all_data.at[idx, 'conflict_status'] = sync_info['status']
                        
                        has_conflicts = sync_info['status'] == "Mâu thuẫn"
                        if 'has_conflicts' in self.all_data.columns:
                            self.all_data.at[idx, 'has_conflicts'] = has_conflicts
                            
                        break
        except Exception as e:
            logger.error(f"Lỗi cập nhật thông tin xung đột: {str(e)}")
        
    def _check_conflict_update_needed(self):
        """
        Check if conflicting information needs to be updated

        Returns:
        bool: True if update is needed, False otherwise
        """
        try:
            if not hasattr(self, '_last_conflict_check'):
                self._last_conflict_check = datetime.now() - timedelta(minutes=10)
                return True
            
            if (datetime.now() - self._last_conflict_check).total_seconds() > 120:
                self._last_conflict_check = datetime.now()
                
                if self.current_doc_id:
                    document = self.data_manager.get_document_by_id(self.current_doc_id)
                    if document:
                        has_conflicts = document.get('has_conflicts', False)
                        conflict_status = document.get('conflict_status', 'Không mâu thuẫn')
                        
                        if (has_conflicts and conflict_status != 'Mâu thuẫn') or \
                        (not has_conflicts and conflict_status == 'Mâu thuẫn'):
                            return True
                        
                        conflict_info = document.get('conflict_info')
                        if has_conflicts and (not conflict_info or conflict_info == '{}'):
                            return True
                
                return True
                
            return False
        except Exception as e:
            logger.error(f"Error checking for conflicting updates: {str(e)}")
            return False
        
    def get_approval_badge(self, status):
        """
        Generate HTML badge for approval status
        """
        status_map = {
            'Pending': ('#FFA500', 'Pending'),
            'Approved': ('#28a745', 'Approved'),
            'Rejected': ('#dc3545', 'Rejected')
        }
        
        color, text = status_map.get(status, ('#718096', status))
        return f'<span style="background-color: {color}; color: white; padding: 4px 12px; border-radius: 16px; font-size: 12px; font-weight: bold;">{text}</span>'

    def on_row_click(self, event):
        try:
            current_time = time.time()
            
            if hasattr(self, '_last_click_time') and current_time - self._last_click_time < 0.3:
                return
                
            if hasattr(self, '_is_handling_click') and self._is_handling_click:
                return
                
            self._is_handling_click = True
            self._last_click_time = current_time
            
            if event.row is None or event.row >= len(self.all_data):
                self._is_handling_click = False
                return
            
            def delayed_process():
                try:
                    if not hasattr(self, 'loading_indicator'):
                        return
                        
                    self.loading_indicator.value = True
                    
                    selected_row = self.all_data.iloc[event.row]
                    doc_id = selected_row['id']
                    
                    if doc_id == getattr(self, 'current_doc_id', None):
                        if hasattr(self, 'update_detail_view'):
                            self.update_detail_view(event.row)
                        return
                    
                    self.update_button_states(selected_row)
                    self.data_table.selection = [event.row]
                    self.current_doc_id = doc_id
                    self.tabs.visible = True
                    
                    self._is_loading_chunks = False 
                    self._is_loading_tab = False
                    self._is_updating = False
                    
                    active_tab = self.tabs.active if hasattr(self, 'tabs') else 0
                    self.load_tab_data(active_tab)
                except Exception as e:
                    logger.error(f"Error in delayed processing: {str(e)}")
                finally:
                    if hasattr(self, 'loading_indicator'):
                        self.loading_indicator.value = False
                    self._is_handling_click = False
            
            try:
                if hasattr(pn.state, 'add_timeout_callback'):
                    pn.state.add_timeout_callback(delayed_process, 50)
                elif hasattr(pn, 'callbacks') and hasattr(pn.callbacks, 'timeout'):
                    pn.callbacks.timeout(50, delayed_process)
                else:
                    delayed_process()
            except AttributeError:
                delayed_process()
            
        except Exception as e:
            logger.error(f"Error handling row click: {str(e)}")
            logger.error(traceback.format_exc())
            self._is_handling_click = False
            if hasattr(self, 'loading_indicator'):
                self.loading_indicator.value = False
    
    def update_detail_view(self, selected_index=None):
        try:
            if selected_index is None and self.data_table.selection:
                selected_index = self.data_table.selection[0]
                
            if selected_index is None or selected_index >= len(self.all_data):
                self.clear_detail_view()
                return

            selected_row = self.all_data.iloc[selected_index]
            doc_id = selected_row['id']
            content = selected_row.get('content', '')
            
            cache_key = f"{doc_id}_detail_view"
            cached_data = self._get_from_cache(cache_key)
            
            formatted_content = self._format_content(content)
            
            self._render_basic_detail_view(selected_row, formatted_content)
            
            if cached_data and 'qa_content' in cached_data:
                self._add_qa_section(cached_data['qa_content'])
            
            try:
                if hasattr(pn.state, 'add_timeout_callback'):
                    pn.state.add_timeout_callback(
                        lambda: self._load_qa_content_background(doc_id, cache_key),
                        200
                    )
                elif hasattr(pn, 'callbacks') and hasattr(pn.callbacks, 'timeout'):
                    pn.callbacks.timeout(200, lambda: self._load_qa_content_background(doc_id, cache_key))
                else:
                    import threading
                    thread = threading.Thread(
                        target=self._load_qa_content_background,
                        args=(doc_id, cache_key),
                        daemon=True
                    )
                    thread.start()
            except Exception as async_error:
                logger.warning(f"Could not schedule background loading: {str(async_error)}")
                self._load_qa_content_background(doc_id, cache_key)
            
            self.save_button.visible = True

        except Exception as e:
            logger.error(f"Error updating detail view: {str(e)}")
            logger.error(traceback.format_exc())
            self.clear_detail_view()
    
    def _format_content(self, content):
        formatted_content = ""
        if content:
            lines = content.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('#'): 
                    formatted_content += f"\n{line}\n\n"
                elif line:  
                    formatted_content += f"{line}\n\n"
                else:  
                    formatted_content += "\n"
                    
            formatted_content = formatted_content.replace('\n\n\n', '\n\n')
        
        return formatted_content

    def _render_basic_detail_view(self, selected_row, formatted_content):
        approval_status = selected_row.get('approval_status', 'Chờ duyệt')
        approval_date = selected_row.get('approval_date', '')
        if pd.isna(approval_date) or approval_date == 'None' or approval_date == 'NaT' or not approval_date:
            approval_date = ''
            
        approver = selected_row.get('approver', '') 
        approver = '' if approver == 'None' else approver

        status_html = self.get_approval_badge(approval_status)

        general_info = pn.Column(
            pn.pane.Markdown("### THÔNG TIN TÀI LIỆU", styles={
                'color': '#2c5282',
                'font-size': '18px',
                'margin-bottom': '10px',
                'font-weight': 'bold'
            }),
            pn.Row(
                pn.Column(
                    pn.pane.Markdown(
                        f"**ID:** {selected_row.get('id', '')}\n"  
                        f"**Ngày tạo:** {selected_row.get('created_date', '')}",
                        styles={
                            'font-size': '14px',
                            'background': '#f8fafc',
                            'padding': '15px',
                            'border-radius': '8px', 
                            'border-left': '4px solid #4299e1',
                            'margin': '5px 0'
                        }
                    ),
                    pn.pane.Markdown(
                        "**Trạng thái:**",
                        styles={
                            'font-size': '14px',
                            'margin-bottom': '5px',
                            'margin-top': '10px'
                        }
                    ),
                    pn.pane.HTML(status_html),
                    width=400
                ),
                pn.Column(
                    pn.pane.Markdown(
                        f"**Người gửi:** {selected_row.get('sender', '')}\n"
                        f"**Ngày duyệt:** {approval_date}\n" 
                        f"**Người duyệt:** {approver}",
                        styles={
                            'font-size': '14px',
                            'background': '#f8fafc',
                            'padding': '15px', 
                            'border-radius': '8px',
                            'border-left': '4px solid #4299e1',
                            'margin': '5px 0'
                        }
                    ),
                    width=400
                ),
                sizing_mode='stretch_width'
            )
        )

        content_section = pn.Column(
            pn.pane.Markdown("### NỘI DUNG GỐC", styles={
                'color': '#2c5282',
                'font-size': '18px',
                'font-weight': 'bold',
                'margin-top': '25px',
                'margin-bottom': '10px'
            }),
            pn.pane.Markdown(
                formatted_content,
                styles={
                    'background': '#f8fafc',
                    'padding': '20px',
                    'border-radius': '8px',
                    'font-size': '14px',
                    'line-height': '1.6',
                    'border-left': '4px solid #4299e1',
                    'white-space': 'pre-wrap',
                    'word-break': 'break-word',
                    'margin-bottom': '20px'
                }
            ),
            sizing_mode='stretch_width'
        )

        self.detail_view[:] = [general_info, content_section]
    
    def _load_qa_content_background(self, doc_id, cache_key):
        try:
            processed_content = ""
            chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
            if chunks:
                qa_parts = []
                for chunk in chunks:
                    metadata = chunk.get('metadata', {})
                    revised_chunk = metadata.get('revised_chunk', '')
                    
                    if revised_chunk:
                        qa_pairs = self.extract_qa_pairs(revised_chunk)
                        for q, a in qa_pairs:
                            qa_parts.append(f"{q}\n{a}\n")
                    else:
                        qa_content = chunk.get('qa_content', '')
                        if qa_content:
                            qa_pairs = self.extract_qa_pairs(qa_content)
                            for q, a in qa_pairs:
                                qa_parts.append(f"{q}\n{a}\n")
                
                processed_content = "\n".join(qa_parts)
                
                self._update_cache(cache_key, {'qa_content': processed_content}, priority=2)
                
                if self.current_doc_id == doc_id:
                    self._add_qa_section(processed_content)
        except Exception as e:
            logger.error(f"Error loading QA content in background: {str(e)}")

    def _add_qa_section(self, processed_content):
        if processed_content and len(processed_content.strip()) > 0:
            existing_qa = False
            for component in self.detail_view:
                if hasattr(component, 'name') and component.name == 'qa_section':
                    existing_qa = True
                    component[1].object = processed_content  
                    break
                    
            if not existing_qa:
                qa_content_section = pn.Column(
                    pn.pane.Markdown("### THÔNG TIN HỎI ĐÁP", styles={
                        'color': '#2c5282',
                        'font-size': '18px',
                        'font-weight': 'bold',
                        'margin-top': '15px',
                        'margin-bottom': '10px'
                    }),
                    pn.pane.Markdown(
                        processed_content,
                        styles={
                            'background': '#edf8f6',
                            'padding': '20px',
                            'border-radius': '8px',
                            'font-size': '14px',
                            'line-height': '1.6',
                            'border-left': '4px solid #38b2ac',
                            'white-space': 'pre-wrap',
                            'word-break': 'break-word'
                        }
                    ),
                    name='qa_section',
                    sizing_mode='stretch_width'
                )
                
                self.detail_view.append(qa_content_section)
        
    def show_error_message(self, message):
        """
        Display an error message in the chunk error container.

        Args:
            message (str): The error message to display.

        Returns:
            None
        """
        self.chunk_error_container.objects[0].object = f"❌ {message}"
        self.chunk_error_container.visible = True

    def show_info_message(self, message):
        """
        Display an informational message in the chunk info container.

        Args:
            message (str): The informational message to display.

        Returns:
            None
        """
        self.chunk_info_container.objects[0].object = f"ℹ️ {message}"
        self.chunk_info_container.visible = True

    def clear_messages(self):
        """
        Reset all views and components to their initial state.

        Returns:
            None
        """
        self.chunk_info_container.objects[0].object = ""
        self.chunk_error_container.objects[0].object = ""
        self.chunk_info_container.visible = False
        self.chunk_error_container.visible = False

    def clear_detail_view(self):
        """
        Clear the detail view and reset related UI components.

        It handles the following tasks:
        - Replaces the current content of the detail view with a default message.
        - Hides the save button and tabs.
        - Clears any data in the chunks and similar documents tables, and hides them.
        - Clears any conflicts that may be displayed.
        - Resets the document ID and clears any messages shown in the UI.

        Returns:
            None
        """
        try:
            self.detail_view[:] = [
                pn.pane.Markdown("### THÔNG TIN TÀI LIỆU", styles={
                    'color': '#2c5282',
                    'font-size': '18px',
                    'font-weight': 'bold',
                    'margin-bottom': '10px'
                }),
                pn.pane.Markdown(
                    "Chưa có tài liệu được chọn. Vui lòng chọn một tài liệu từ danh sách.", 
                    styles={
                        'font-style': 'italic',
                        'color': '#718096',
                        'background': '#f7fafc',
                        'padding': '20px',
                        'border-radius': '8px',
                        'text-align': 'center',
                        'margin': '20px 0',
                        'border': '1px dashed #cbd5e0'
                    }
                )
            ]
            
            self.save_button.visible = False
            self.tabs.visible = False
            
            if hasattr(self, 'chunks_table'):
                self.chunks_table.value = pd.DataFrame()
                self.chunks_table.visible = False
            
            if hasattr(self, 'similar_docs_table'):
                self.similar_docs_table.value = pd.DataFrame()
                self.similar_docs_table.visible = False

            if hasattr(self, 'conflicts_container'):
                self.conflicts_container.clear()
            
            self.clear_messages()
            self.current_doc_id = None
            
        except Exception as e:
            logger.error(f"Error clearing detail view: {str(e)}")

    def approve_document(self, event=None):
        try:
            if not self.data_table.selection:
                self.show_notification("Vui lòng chọn một tài liệu để duyệt", alert_type="error")
                return
                    
            self.loading_indicator.value = True
            self._is_updating = True
            
            self.approve_button.disabled = True
            self.reject_button.disabled = True
            self.delete_button.disabled = True
                    
            selected_index = self.data_table.selection[0]
            selected_row = self.all_data.iloc[selected_index]
            doc_id = selected_row['id']
            doc_unit = selected_row['unit']

            if doc_unit != self.unit:
                self.show_notification(f"Bạn không có quyền duyệt tài liệu của đơn vị {doc_unit}", alert_type="error")
                self._is_updating = False
                self.loading_indicator.value = False
                self.update_button_states(selected_row)
                return

            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                self.show_notification("Không tìm thấy thông tin tài liệu", alert_type="error")
                self._is_updating = False
                self.loading_indicator.value = False
                self.update_button_states(selected_row)
                return
            
            try:
                success = self.data_manager.update_document_approval(doc_id, self.username)
                
                if success:
                    self.show_notification(f"Tài liệu {doc_id} đã được duyệt thành công!", alert_type="success")
                    
                    status_filter = self.doc_type_selector.value if hasattr(self, 'doc_type_selector') else "All"
                    
                    if status_filter == "All":
                        self.all_data = self.data_manager.get_all_documents()
                    else:
                        self.all_data = self.data_manager.get_filtered_data(status=status_filter)
                    
                    self._format_initial_data()
                    
                    available_columns = [col for col in self.displayed_columns if col in self.all_data.columns]
                    self.data_table.value = self.all_data[available_columns]
                    
                    new_index = None
                    for idx, row in self.all_data.iterrows():
                        if row['id'] == doc_id:
                            new_index = idx
                            break
                    
                    if new_index is not None:
                        self.data_table.selection = [new_index]
                        selected_row = self.all_data.iloc[new_index]
                        
                        if self.current_doc_id == doc_id:
                            self.update_detail_view(new_index)
                    
                        self.update_button_states(selected_row)
                    else:
                        self.current_doc_id = None
                        self.clear_detail_view()
                        self.approve_button.disabled = True
                        self.reject_button.disabled = True
                        self.delete_button.disabled = True
                else:
                    self.show_notification("Không thể duyệt tài liệu. Vui lòng thử lại sau.", alert_type="error")
                    self.update_button_states(selected_row)
            except Exception as update_error:
                logger.error(f"Error updating approval status: {str(update_error)}")
                self.show_notification(f"Lỗi cập nhật: {str(update_error)}", alert_type="error")
                self.update_button_states(selected_row)

        except Exception as e:
            logger.error(f"Error while approving document: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_notification("Lỗi hệ thống khi duyệt tài liệu", alert_type="error")
            
            if self.data_table.selection:
                selected_index = self.data_table.selection[0]
                if selected_index < len(self.all_data):
                    selected_row = self.all_data.iloc[selected_index]
                    self.update_button_states(selected_row)
            
        finally:
            self.loading_indicator.value = False
            self._is_updating = False

    def reject_document(self, event=None):
        try:
            if not self.data_table.selection:
                self.show_notification("Vui lòng chọn một tài liệu để từ chối", alert_type="error")
                return
                    
            self.loading_indicator.value = True
            self._is_updating = True
            
            self.approve_button.disabled = True
            self.reject_button.disabled = True
            self.delete_button.disabled = True
                    
            selected_index = self.data_table.selection[0]
            selected_row = self.all_data.iloc[selected_index]
            doc_id = selected_row['id']
            doc_unit = selected_row['unit']

            if doc_unit != self.unit:
                self.show_notification(f"Bạn không có quyền từ chối tài liệu của đơn vị {doc_unit}", alert_type="error")
                self._is_updating = False
                self.loading_indicator.value = False
                self.update_button_states(selected_row)
                return

            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                self.show_notification("Không tìm thấy thông tin tài liệu", alert_type="error")
                self._is_updating = False
                self.loading_indicator.value = False
                self.update_button_states(selected_row)
                return
            
            try:
                success = self.data_manager.update_document_rejection(doc_id, self.username)
                
                if success:
                    self.show_notification(f"Tài liệu {doc_id} đã bị từ chối thành công!", alert_type="success")
                    
                    status_filter = self.doc_type_selector.value if hasattr(self, 'doc_type_selector') else "All"
                    
                    if status_filter == "All":
                        self.all_data = self.data_manager.get_all_documents()
                    else:
                        self.all_data = self.data_manager.get_filtered_data(status=status_filter)
                    
                    self._format_initial_data()
                    
                    available_columns = [col for col in self.displayed_columns if col in self.all_data.columns]
                    self.data_table.value = self.all_data[available_columns]
                    
                    new_index = None
                    for idx, row in self.all_data.iterrows():
                        if row['id'] == doc_id:
                            new_index = idx
                            break
                    
                    if new_index is not None:
                        self.data_table.selection = [new_index]
                        selected_row = self.all_data.iloc[new_index]
                        
                        if self.current_doc_id == doc_id:
                            self.update_detail_view(new_index)
                    
                        self.update_button_states(selected_row)
                    else:
                        self.current_doc_id = None
                        self.clear_detail_view()
                        self.approve_button.disabled = True
                        self.reject_button.disabled = True
                        self.delete_button.disabled = True
                else:
                    self.show_notification("Không thể từ chối tài liệu. Vui lòng thử lại sau.", alert_type="error")
                    self.update_button_states(selected_row)
            except Exception as update_error:
                logger.error(f"Error updating rejection status: {str(update_error)}")
                self.show_notification(f"Lỗi cập nhật: {str(update_error)}", alert_type="error")
                self.update_button_states(selected_row)

        except Exception as e:
            logger.error(f"Error while rejecting document: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_notification("Lỗi hệ thống khi từ chối tài liệu", alert_type="error")
            
            if self.data_table.selection:
                selected_index = self.data_table.selection[0]
                if selected_index < len(self.all_data):
                    selected_row = self.all_data.iloc[selected_index]
                    self.update_button_states(selected_row)
            
        finally:
            self.loading_indicator.value = False
            self._is_updating = False

    def _update_after_delete(self):
        """
        Update interface after deleting document
        """
        try:
            was_updating = False
            if hasattr(self, 'update_callback'):
                try:
                    if self.update_callback is not None:
                        self.update_callback.stop()
                        was_updating = True
                except Exception as stop_error:
                    logger.warning(f"Error stopping update callback: {str(stop_error)}")
                
            self._is_updating = True
                
            try:
                status_filter = self.doc_type_selector.value if hasattr(self, 'doc_type_selector') else "All"
                
                if status_filter == "All":
                    self.all_data = self.data_manager.get_all_documents()
                else:
                    self.all_data = self.data_manager.get_filtered_data(status=status_filter)
                    
                logger.info(f"Fetched {len(self.all_data)} rows after deletion")
            except Exception as db_error:
                logger.error(traceback.format_exc())
                self.all_data = pd.DataFrame(columns=self.displayed_columns)
            
            try:
                if len(self.all_data) > 0:
                    if 'is_duplicate' in self.all_data.columns:
                        self.all_data['is_duplicate'] = self.all_data['is_duplicate'].apply(
                            lambda x: "Duplicate" if x else "Not duplicate"
                        )
            except Exception as format_error:
                logger.error(f"Lỗi khi định dạng dữ liệu: {str(format_error)}")
                logger.error(traceback.format_exc())
                        
            try:
                available_columns = [col for col in self.displayed_columns if col in self.all_data.columns]
                
                if len(self.all_data) == 0:
                    empty_df = pd.DataFrame(columns=available_columns)
                    self.data_table.selection = []
                    self.data_table.value = empty_df
                else:
                    self.data_table.selection = []
                    self.data_table.value = self.all_data[available_columns]
                    
                logger.info(f"Updated table with {len(self.data_table.value)} rows")
            except Exception as table_error:
                logger.error(f"Lỗi khi cập nhật bảng: {str(table_error)}")
                logger.error(traceback.format_exc())
            
            try:
                self.clear_detail_view()
                
                if hasattr(self, 'conflicts_container'):
                    self.conflicts_container.clear()
                    self.conflicts_container.append(
                        pn.pane.Markdown(
                            "### Không có thông tin mâu thuẫn\nTài liệu đã bị xóa hoặc chưa có tài liệu được chọn.",
                            styles={
                                'color': '#4A5568',
                                'text-align': 'center',
                                'padding': '20px',
                                'background': '#EDF2F7',
                                'border-radius': '8px',
                                'margin': '20px 0',
                                'font-size': '16px'
                            }
                        )
                    )
                
                if hasattr(self, 'current_doc_id'):
                    self.current_doc_id = None
                
                if hasattr(self, 'tabs'):
                    self.tabs.visible = False
                    
                self.update_button_states(None)
                
            except Exception as clear_error:
                logger.error(f"Error while clearing selection: {str(clear_error)}")
            
            try:
                if hasattr(self.data_table, 'param'):
                    self.data_table.param.trigger('value')
            except Exception as trigger_error:
                logger.error(f"Error while triggering param: {str(trigger_error)}")
                
            self._is_updating = False
                
            if was_updating:
                try:
                    self.setup_auto_update(period=5000) 
                except Exception as update_error:
                    logger.error(f"Error restarting auto update: {str(update_error)}")
            
        except Exception as e:
            logger.error(f"Overall error in _update_after_delete: {str(e)}")
            logger.error(traceback.format_exc())
            if hasattr(self, '_is_updating'):
                self._is_updating = False
            
            try:
                pn.state.notifications.error("Lỗi khi cập nhật giao diện sau khi xóa")
            except:
                pass

    def _create_conflict_card(self, conflict, conflict_type="internal"):
        """
        Create a simplified card displaying conflict information without resolution controls
        to avoid triggering unnecessary conflict analysis
        
        Args:
            conflict: Dict containing conflict information
            conflict_type: Conflict type ("content", "internal" or "external")
                
        Returns:
            Panel Card: Card displaying conflict information
        """
        try:
            if not isinstance(conflict, dict):
                logger.warning(f"Invalid conflict data: {conflict}")
                return pn.pane.Markdown(
                    "**Dữ liệu mâu thuẫn không hợp lệ**",
                    styles={
                        'color': '#dc2626',
                        'background': '#fee2e2',
                        'padding': '10px',
                        'border-radius': '4px',
                        'margin': '5px 0'
                    }
                )
            
            if conflict_type == "content" and "contradictions" in conflict:
                contradictions = conflict.get("contradictions", [])
                if contradictions:
                    cards = []
                    for contradiction in contradictions:
                        contradiction_card = self._create_single_contradiction_card(
                            contradiction, 
                            conflict.get("chunk_ids", []), 
                            conflict.get("analyzed_at", "")
                        )
                        cards.append(contradiction_card)
                    
                    if cards:
                        return pn.Column(*cards)
                        
            explanation = conflict.get('explanation', 'Không có giải thích')
            conflicting_parts = conflict.get('conflicting_parts', [])
            chunk_ids = conflict.get('chunk_ids', [])
            analyzed_at = conflict.get('analyzed_at', '')
            severity = conflict.get('severity', 'medium')
            
            if analyzed_at:
                try:
                    if isinstance(analyzed_at, str):
                        try:
                            analyzed_at = datetime.fromisoformat(analyzed_at)
                            formatted_time = format_date(analyzed_at)
                        except ValueError:
                            formatted_time = analyzed_at
                    else:
                        formatted_time = format_date(analyzed_at)
                except Exception:
                    formatted_time = str(analyzed_at)
            else:
                formatted_time = "Không rõ"
            
            severity_colors = {
                'high': {'bg': '#fee2e2', 'text': '#dc2626', 'border': '#f87171'},
                'medium': {'bg': '#fef3c7', 'text': '#d97706', 'border': '#fbbf24'},
                'low': {'bg': '#e0f2fe', 'text': '#0284c7', 'border': '#38bdf8'}
            }
            
            severity_style = severity_colors.get(severity, severity_colors['medium'])
            
            conflict_type_labels = {
                "content": "Mâu thuẫn nội dung chunk",
                "internal": "Mâu thuẫn nội bộ",
                "external": "Mâu thuẫn ngoại bộ"
            }
            
            conflict_type_label = conflict_type_labels.get(conflict_type, "Mâu thuẫn")
            
            header = pn.pane.Markdown(
                f"### {conflict_type_label}: {explanation}",
                styles={
                    'color': severity_style['text'],
                    'font-weight': 'bold',
                    'margin-bottom': '15px',
                    'background': severity_style['bg'],
                    'padding': '12px',
                    'border-radius': '4px'
                },
                sizing_mode='stretch_width'
            )
            
            parts_content = []
            
            if conflict_type == "external" and len(chunk_ids) >= 2:
                doc_chunks = {}
                for chunk_id in chunk_ids:
                    if '_paragraph_' in chunk_id:
                        doc_id = chunk_id.split('_paragraph_')[0]
                        if doc_id not in doc_chunks:
                            doc_chunks[doc_id] = []
                        doc_chunks[doc_id].append(chunk_id)
                
                doc_metadata = {}
                for doc_id in doc_chunks.keys():
                    try:
                        doc = self.data_manager.get_document_by_id(doc_id)
                        if doc:
                            created_date = ""
                            if 'created_date' in doc and doc['created_date']:
                                if isinstance(doc['created_date'], str):
                                    created_date = doc['created_date']
                                else:
                                    try:
                                        created_date = format_date(doc.get('created_date'))
                                    except:
                                        created_date = str(doc['created_date'])
                            
                            doc_metadata[doc_id] = {
                                'id': doc_id,
                                'created_date': created_date,
                                'sender': doc.get('sender', ''),
                                'unit': doc.get('unit', '')
                            }
                    except Exception as doc_error:
                        logger.warning(f"Error fetching document metadata for {doc_id}: {str(doc_error)}")
                
                if doc_metadata:
                    parts_content.append(
                        pn.pane.Markdown(
                            "**Các tài liệu liên quan:**",
                            styles={
                                'margin-top': '15px',
                                'margin-bottom': '10px',
                                'font-weight': 'bold',
                                'color': '#4b5563',
                                'font-size': '16px'
                            }
                        )
                    )
                    
                    doc_cards = []
                    for doc_id, metadata in doc_metadata.items():
                        doc_card = pn.pane.Markdown(
                            f"**Tài liệu:** {metadata['id']}\n"
                            f"**Ngày tạo:** {metadata['created_date']}\n"
                            f"**Người gửi:** {metadata['sender']}\n"
                            f"**Đơn vị:** {metadata['unit']}",
                            styles={
                                'background': '#f1f5f9',
                                'padding': '12px',
                                'border-radius': '6px',
                                'margin': '8px 0',
                                'font-size': '14px',
                                'border-left': '4px solid #94a3b8'
                            }
                        )
                        doc_cards.append(doc_card)
                    
                    if len(doc_cards) == 2:
                        doc_container = pn.Row(
                            pn.Column(doc_cards[0], width=500, margin=(0, 20, 0, 0)),
                            pn.Column(doc_cards[1], width=500),
                            sizing_mode='stretch_width'
                        )
                        parts_content.append(doc_container)
                    else:
                        doc_container = pn.Column(*doc_cards, margin=(0, 0, 15, 0))
                        parts_content.append(doc_container)
                    
                    parts_content.append(
                        pn.pane.Markdown(
                            "**Nội dung mâu thuẫn**",
                            styles={
                                'margin-top': '20px',
                                'margin-bottom': '10px',
                                'font-weight': 'bold',
                                'color': '#4b5563',
                                'font-size': '16px'
                            }
                        )
                    )
                    
                    if len(conflicting_parts) == 2 and len(doc_chunks) == 2:
                        doc_ids = list(doc_chunks.keys())
                        
                        comparison = pn.Row(
                            pn.Column(
                                pn.pane.Markdown(
                                    f"**Tài liệu: {doc_ids[0]}**",
                                    styles={
                                        'font-weight': 'bold', 
                                        'color': '#2563eb',
                                        'font-size': '15px',
                                        'margin-bottom': '8px'
                                    }
                                ),
                                pn.pane.Markdown(
                                    conflicting_parts[0],
                                    styles={
                                        'background': '#f9fafb',
                                        'padding': '15px',
                                        'border-radius': '6px',
                                        'margin': '5px 0',
                                        'font-size': '15px',
                                        'line-height': '1.6',
                                        'border-left': '4px solid #2563eb'
                                    }
                                ),
                                width=500,
                                margin=(0, 30, 0, 0)  
                            ),
                            pn.Column(
                                pn.pane.Markdown(
                                    f"**Tài liệu: {doc_ids[1]}**",
                                    styles={
                                        'font-weight': 'bold', 
                                        'color': '#2563eb',
                                        'font-size': '15px',
                                        'margin-bottom': '8px'
                                    }
                                ),
                                pn.pane.Markdown(
                                    conflicting_parts[1],
                                    styles={
                                        'background': '#f9fafb',
                                        'padding': '15px',
                                        'border-radius': '6px',
                                        'margin': '5px 0',
                                        'font-size': '15px',
                                        'line-height': '1.6',
                                        'border-left': '4px solid #2563eb'
                                    }
                                ),
                                width=500
                            ),
                            sizing_mode='stretch_width'
                        )
                        parts_content.append(comparison)
                    else:
                        for i, part in enumerate(conflicting_parts):
                            doc_id = list(doc_metadata.keys())[i % len(doc_metadata)] if i < len(doc_metadata) else "Unknown"
                            parts_content.append(
                                pn.pane.Markdown(
                                    f"**Từ tài liệu {doc_id}:**\n\n{part}",
                                    styles={
                                        'background': '#f9fafb',
                                        'padding': '15px',
                                        'border-radius': '6px',
                                        'margin': '10px 0',
                                        'font-size': '15px',
                                        'line-height': '1.6',
                                        'border-left': '4px solid #4b5563'
                                    }
                                )
                            )
                else:
                    for i, part in enumerate(conflicting_parts):
                        parts_content.append(
                            pn.pane.Markdown(
                                f"**Phần mâu thuẫn {i+1}:** {part}",
                                styles={
                                    'background': '#f9fafb',
                                    'padding': '15px',
                                    'border-radius': '6px',
                                    'margin': '10px 0',
                                    'font-size': '15px',
                                    'line-height': '1.6',
                                    'border-left': '4px solid #4b5563'
                                }
                            )
                        )
            else:
                parts_content.append(
                    pn.pane.Markdown(
                        f"**Chi tiết {conflict_type_label.lower()}:**",
                        styles={
                            'margin-top': '10px',
                            'margin-bottom': '10px',
                            'font-weight': 'bold',
                            'color': '#4b5563',
                            'font-size': '16px'
                        }
                    )
                )
                
                for i, part in enumerate(conflicting_parts):
                    parts_content.append(
                        pn.pane.Markdown(
                            f"**Phần mâu thuẫn {i+1}:** {part}",
                            styles={
                                'background': '#f9fafb',
                                'padding': '15px',
                                'border-radius': '6px',
                                'margin': '10px 0',
                                'font-size': '15px',
                                'line-height': '1.6',
                                'border-left': '4px solid #4b5563'
                            }
                        )
                    )
            
            chunk_info = pn.pane.Markdown(
                f"**Chunk liên quan:** {', '.join(chunk_ids) if chunk_ids else 'Không có'}\n\n"
                f"**Thời gian phân tích:** {formatted_time}",
                styles={
                    'font-size': '13px',
                    'color': '#4b5563',
                    'margin-top': '15px',
                    'padding': '8px',
                    'background': '#f8fafc',
                    'border-radius': '4px'
                }
            )

            return pn.Card(
                header,
                pn.Column(*parts_content),
                chunk_info,
                styles={
                    'background': '#ffffff',
                    'border': f'1px solid {severity_style["border"]}',
                    'border-left': '4px solid #e53e3e', 
                    'border-radius': '8px',
                    'padding': '20px',
                    'margin': '15px 0',
                    'box-shadow': '0 2px 4px rgba(0,0,0,0.1)'
                }
            )
        except Exception as e:
            logger.error(f"Error creating conflict card: {str(e)}")
            logger.error(traceback.format_exc())
            return pn.pane.Markdown(f"Lỗi hiển thị thông tin mâu thuẫn: {str(e)}")

    def _create_single_contradiction_card(self, contradiction, chunk_ids=None, analyzed_at=""):
        """
        Create a card for a single contradiction in contradictions list
        
        Args:
            contradiction: Dict containing info about a specific contradiction
            chunk_ids: List of related chunk IDs
            analyzed_at: Analysis timestamp
                
        Returns:
            Panel Card: Card displaying single contradiction
        """
        try:
            contradiction_id = contradiction.get('id', 0)
            description = contradiction.get('description', 'Không có mô tả')
            explanation = contradiction.get('explanation', 'Không có giải thích')
            conflicting_parts = contradiction.get('conflicting_parts', [])
            severity = contradiction.get('severity', 'medium')
            
            formatted_time = format_date(analyzed_at, default_text="Không rõ")
            
            severity_colors = {
                'high': {'bg': '#fee2e2', 'text': '#dc2626', 'border': '#f87171'},
                'medium': {'bg': '#fef3c7', 'text': '#d97706', 'border': '#fbbf24'},
                'low': {'bg': '#e0f2fe', 'text': '#0284c7', 'border': '#38bdf8'}
            }
            
            severity_style = severity_colors.get(severity, severity_colors['medium'])
            
            header = pn.pane.Markdown(
                f"### Mâu thuẫn #{contradiction_id}: {description}",
                styles={
                    'color': severity_style['text'],
                    'font-weight': 'bold',
                    'margin-bottom': '10px',
                    'background': severity_style['bg'],
                    'padding': '8px',
                    'border-radius': '4px'
                }
            )
            
            explanation_text = pn.pane.Markdown(
                f"**Giải thích:** {explanation}",
                styles={
                    'margin': '10px 0',
                    'font-size': '14px',
                    'background': '#f7fafc',
                    'padding': '8px',
                    'border-radius': '4px'
                }
            )
            
            parts_content = []
            parts_header = pn.pane.Markdown(
                "**Chi tiết phần mâu thuẫn:**",
                styles={
                    'margin-top': '15px',
                    'margin-bottom': '10px',
                    'font-weight': 'bold',
                    'color': '#4b5563',
                    'font-size': '15px'
                }
            )
            parts_content.append(parts_header)
            
            for i, part in enumerate(conflicting_parts):
                parts_content.append(
                    pn.pane.Markdown(
                        f"**Phần mâu thuẫn {i+1}:** {part}",
                        styles={
                            'background': '#f9fafb',
                            'padding': '10px',
                            'border-radius': '4px',
                            'margin': '5px 0',
                            'font-size': '14px',
                            'border-left': '4px solid #4b5563'
                        }
                    )
                )
            
            chunk_info = pn.pane.Markdown(
                f"**Chunk liên quan:** {', '.join(chunk_ids) if chunk_ids else 'Không có'}\n\n"
                f"**Thời gian phân tích:** {formatted_time}",
                styles={
                    'font-size': '13px',
                    'color': '#4b5563',
                    'margin-top': '10px',
                    'padding': '8px',
                    'background': '#f8fafc',
                    'border-radius': '4px'
                }
            )
            
        
            conflict_id = '_'.join(chunk_ids) if chunk_ids else None
            
        
            return pn.Card(
                header,
                explanation_text,
                pn.Column(*parts_content),
                chunk_info,
                styles={
                    'background': '#ffffff',
                    'border': f'1px solid {severity_style["border"]}',
                    'border-radius': '8px',
                    'padding': '15px',
                    'margin': '10px 0',
                    'box-shadow': '0 1px 3px rgba(0,0,0,0.1)'
                }
            )
        except Exception as e:
            logger.error(f"Error creating single contradiction card: {str(e)}")
            return pn.pane.Markdown(f"Lỗi hiển thị thông tin mâu thuẫn: {str(e)}")
    
    def _create_no_conflicts_message(self, conflict_type):
        """Create a message card for when no conflicts are found.
        
        Args:
            conflict_type (str): Type of conflict to display in the message
            
        Returns:
            Panel Markdown: A formatted message card
        """
        conflict_type_labels = {
            "content": "mâu thuẫn nội dung",
            "internal": "mâu thuẫn nội bộ",
            "external": "mâu thuẫn ngoại bộ",
            "nội dung": "mâu thuẫn nội dung",
            "nội bộ": "mâu thuẫn nội bộ", 
            "ngoại bộ": "mâu thuẫn ngoại bộ"
        }
        
        emoji_map = {
            "content": "📝",
            "internal": "🔄",
            "external": "🔗",
            "nội dung": "📝",
            "nội bộ": "🔄", 
            "ngoại bộ": "🔗"
        }
        
        label = conflict_type_labels.get(conflict_type, conflict_type)
        emoji = emoji_map.get(conflict_type, "✅")
        
        return pn.pane.Markdown(
            f"**Không tìm thấy {label} nào trong tài liệu này**",
            styles={
                'color': '#047857',
                'font-size': '15px',
                'margin-top': '20px',
                'text-align': 'center',
                'background': '#ecfdf5',
                'padding': '20px',
                'border-radius': '8px',
                'border': '1px solid #a7f3d0',
                'box-shadow': '0 2px 4px rgba(0, 0, 0, 0.05)'
            }
        )
    
    def _check_chunk_exists(self, chunk_id):
        """
        Check if a chunk still exists in the system

        Args:
        chunk_id (str): ID of the chunk to check

        Returns:
        bool: True if chunk exists, False otherwise
        """
        try:
            if not chunk_id or not isinstance(chunk_id, str):
                return False
                
            if hasattr(self, 'chroma_manager') and self.chroma_manager:
                try:
                    results = self.chroma_manager.collection.get(
                        ids=[chunk_id],
                        include=['metadatas']
                    )
                    
                    if results and results['ids'] and len(results['ids']) > 0:
                        return True
                except Exception as chroma_error:
                    logger.warning(f"Lỗi khi kiểm tra chunk trong Chroma: {str(chroma_error)}")
            
            try:
                check_chunk_query = """
                    SELECT 1 FROM document_chunks WHERE id = %s LIMIT 1
                """
                
                result = self.execute_with_retry(check_chunk_query, (chunk_id,), fetch=True)
                if result and len(result) > 0:
                    return True
            except Exception as db_error:
                logger.warning(f"Lỗi khi kiểm tra chunk trong database: {str(db_error)}")
            
            try:
                if '_paragraph_' in chunk_id:
                    doc_id = chunk_id.split('_paragraph_')[0]
                    if doc_id:
                        doc_exists = self.get_document_by_id(doc_id)
                        if not doc_exists:
                            return False
            except Exception as parse_error:
                logger.warning(f"Lỗi khi phân tích chunk_id: {str(parse_error)}")
                
            return False
            
        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra sự tồn tại của chunk {chunk_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
    
    def confirm_delete(self, event):
        try:
            if not self.data_table.selection:
                self.show_notification("Vui lòng chọn tài liệu để xóa", alert_type="error")
                return

            selected_index = self.data_table.selection[0]
            selected_row = self.all_data.iloc[selected_index]
            doc_id = selected_row['id']
            doc_unit = selected_row['unit']

            if doc_unit != self.unit:
                self.show_notification(f"Bạn không có quyền xóa tài liệu của đơn vị {doc_unit}", alert_type="error")
                return
            
            if hasattr(self, 'loading_indicator'):
                self.loading_indicator.value = True
            
            duplicate_group_id = None
            related_docs = []
            try:
                document = self.data_manager.get_document_by_id(doc_id)
                if document:
                    duplicate_group_id = document.get('duplicate_group_id')
                    if duplicate_group_id:
                        related_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                        related_docs = [d['id'] for d in related_docs if d['id'] != doc_id]
            except Exception as doc_error:
                logger.warning(f"Lỗi khi lấy thông tin tài liệu trước khi xóa: {str(doc_error)}")
            
            was_current = (self.current_doc_id == doc_id)
            
            success = self.data_manager.delete_document(doc_id, chroma_manager=self.chroma_manager)
            
            if success:
                if was_current:
                    self.current_doc_id = None
                    self.clear_detail_view()
                    
                    if hasattr(self, 'conflicts_container'):
                        self.conflicts_container.clear()
                        self.conflicts_container.append(
                            pn.pane.Markdown(
                                "### Tài liệu đã bị xóa\nKhông có thông tin mâu thuẫn để hiển thị.",
                                styles={
                                    'color': '#4A5568',
                                    'text-align': 'center',
                                    'padding': '20px',
                                    'background': '#EDF2F7', 
                                    'border-radius': '8px',
                                    'margin': '20px 0',
                                    'font-size': '16px'
                                }
                            )
                        )
                    
                    if hasattr(self, 'tabs'):
                        self.tabs.visible = False
                
                if related_docs:
                    self.show_notification(f"Đang cập nhật thông tin mâu thuẫn cho {len(related_docs)} tài liệu liên quan...", alert_type="info")
                    
                    for related_id in related_docs:
                        try:
                            formatted_time = format_date(datetime.now(), format_str='%Y-%m-%d %H:%M:%S')
                            resolution_note = f"Tài liệu {doc_id} đã bị xóa vào {formatted_time}"
                            resolve_query = """
                                UPDATE chunk_conflicts
                                SET resolved = TRUE,
                                    resolved_at = CURRENT_TIMESTAMP,
                                    resolution_notes = %s
                                WHERE doc_id = %s AND conflict_id LIKE %s AND resolved = FALSE
                            """
                            self.data_manager.execute_with_retry(
                                resolve_query, 
                                (resolution_note, related_id, f"%{doc_id}%"),
                                fetch=False
                            )
                            
                            document = self.data_manager.get_document_by_id(related_id)
                            if document:
                                conflict_info = document.get('conflict_info', '{}')
                                if isinstance(conflict_info, str):
                                    try:
                                        conflict_info = json.loads(conflict_info)
                                    except:
                                        conflict_info = {}
                                
                                new_conflicts = {
                                    "content_conflicts": [],
                                    "internal_conflicts": [],
                                    "external_conflicts": []
                                }
                                
                                for conflict in conflict_info.get('content_conflicts', []):
                                    if isinstance(conflict, dict):
                                        chunk_id = conflict.get('chunk_id', '')
                                        if not chunk_id.startswith(doc_id):
                                            new_conflicts['content_conflicts'].append(conflict)
                                
                                for conflict in conflict_info.get('internal_conflicts', []):
                                    if isinstance(conflict, dict) and 'chunk_ids' in conflict:
                                        has_deleted_doc = False
                                        for chunk_id in conflict.get('chunk_ids', []):
                                            if chunk_id.startswith(doc_id):
                                                has_deleted_doc = True
                                                break
                                        
                                        if not has_deleted_doc:
                                            new_conflicts['internal_conflicts'].append(conflict)
                                
                                for conflict in conflict_info.get('external_conflicts', []):
                                    if isinstance(conflict, dict) and 'chunk_ids' in conflict:
          
                                        has_deleted_doc = False
                                        for chunk_id in conflict.get('chunk_ids', []):
                                            if chunk_id.startswith(doc_id):
                                                has_deleted_doc = True
                                                break
                                        
                                        if not has_deleted_doc:
                                            new_conflicts['external_conflicts'].append(conflict)
                                
                                has_conflicts = (
                                    len(new_conflicts['content_conflicts']) > 0 or
                                    len(new_conflicts['internal_conflicts']) > 0 or
                                    len(new_conflicts['external_conflicts']) > 0
                                )
                                
                                self.data_manager.update_document_status(related_id, {
                                    'has_conflicts': has_conflicts,
                                    'conflict_info': json.dumps(new_conflicts),
                                    'conflict_status': 'Pending Review' if has_conflicts else 'No Conflict',
                                    'last_conflict_check': datetime.now().isoformat()
                                })
                                
                                logger.info(f"Đã cập nhật trạng thái mâu thuẫn cho tài liệu {related_id}")
                        except Exception as update_error:
                            logger.error(f"Lỗi khi cập nhật mâu thuẫn cho tài liệu {related_id}: {str(update_error)}")
                
                self._update_after_delete()
                self.show_notification(f"Đã xóa thành công tài liệu {doc_id}", alert_type="success")
            else:
                self.show_notification("Không thể xóa tài liệu", alert_type="error")
                
        except Exception as e:
            logger.error(traceback.format_exc())
            self.show_notification("Lỗi hệ thống khi xóa tài liệu", alert_type="error")
        
        finally:
            if hasattr(self, 'loading_indicator'):
                self.loading_indicator.value = False

    def request_reload_conflicts(self, event):
        """
        Request to reload internal conflict information
        """
        try:
            if not self.current_doc_id:
                self.show_notification("Vui lòng chọn một tài liệu", alert_type="warning")
                return
            
            self.request_conflict_analysis(self.current_doc_id)
            
        except Exception as e:
            logger.error(f"Error reloading conflict: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_notification(f"Lỗi khi tải lại mâu thuẫn: {str(e)}", alert_type="error")

    def handle_analysis_result(self, task_id, result_data):
        """
        Handle conflicting analysis results from API and update interface

        Args:
        task_id (str): ID of analysis task
        result_data (dict): Received result data
        """
        try:
            if not self.current_doc_id:
                return
                
            task_status = result_data.get('status')
            
            if task_status == 'completed':
                self.load_conflicts_data(self.current_doc_id)
                self.show_notification("Phân tích mâu thuẫn đã hoàn tất", alert_type="success")
            elif task_status == 'processing':
                pn.state.add_timeout_callback(
                    lambda: self.check_analysis_task(task_id), 
                    2000
                )
            else:
                error_msg = result_data.get('message', 'Không rõ lỗi')
                self.show_notification(f"Lỗi phân tích mâu thuẫn: {error_msg}", alert_type="error")
                
        except Exception as e:
            logger.error(f"Error processing analysis results: {str(e)}")
            logger.error(traceback.format_exc())

    def check_analysis_task(self, task_id):
        """
        Check the status of the conflict analysis task

        Args:
        task_id (str): ID of the task to check
        """
        try:
            if not self.current_doc_id:
                return
                
            conflict_api_url = os.getenv('CONFLICT_API_URL')
            if not conflict_api_url:
                self.load_conflicts_data(self.current_doc_id)
                return
                
            try:
                import requests
                response = requests.get(
                    f"{conflict_api_url}/tasks/{task_id}",
                    timeout=5
                )
                
                if response.status_code == 200:
                    result = response.json()
                    self.handle_analysis_result(task_id, result)
                else:
                    self.load_conflicts_data(self.current_doc_id)
                    
            except Exception as api_error:
                logger.error(f"Error when checking task: {str(api_error)}")
                self.load_conflicts_data(self.current_doc_id)
                
        except Exception as e:
            logger.error(f"Task check error: {str(e)}")
            logger.error(traceback.format_exc())

    def request_conflict_analysis(self, doc_id):
        """
        Gửi yêu cầu phân tích xung đột với các cải tiến hiệu suất
        """
        try:
            if hasattr(self, '_analysis_in_progress') and self._analysis_in_progress:
                self.show_notification("Đang có phân tích đang chạy, vui lòng đợi", alert_type="warning")
                return False
                
            if not hasattr(self, '_analysis_mutex'):
                import threading
                self._analysis_mutex = threading.Lock()
                
            if not self._analysis_mutex.acquire(blocking=False):
                self.show_notification("Đang xử lý, vui lòng đợi", alert_type="warning")
                return False
                
            try:
                self._analysis_in_progress = True
                
                if not doc_id:
                    self.show_notification("Vui lòng chọn một tài liệu", alert_type="warning")
                    self._is_analyzing = False
                    self._analysis_mutex.release()
                    return False
                        
                if hasattr(self, 'conflicts_container'):
                    self.conflicts_container.clear()
                    self.conflicts_container.append(
                        pn.Column(
                            pn.indicators.LoadingSpinner(value=True, size=40),
                            pn.pane.Markdown("### Đang chuẩn bị phân tích mâu thuẫn...", styles={
                                'color': '#4A5568',
                                'text-align': 'center',
                                'padding': '20px',
                                'background': '#EDF2F7',
                                'border-radius': '8px',
                                'margin': '20px 0'
                            }),
                            align='center'
                        )
                    )
                
                document = self.data_manager.get_document_by_id(doc_id)
                if not document:
                    self.show_notification("Không tìm thấy tài liệu", alert_type="error")
                    self._analysis_in_progress = False
                    self._analysis_mutex.release()
                    return False
                        
                duplicate_group_id = document.get('duplicate_group_id')
                
                docs_to_update = [doc_id]
                if duplicate_group_id:
                    try:
                        group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                        if group_docs:
                            docs_to_update = [doc['id'] for doc in group_docs[:3]]
                    except Exception as group_error:
                        logger.error(f"Error getting group docs: {str(group_error)}")
                
                for update_doc_id in docs_to_update:
                    try:
                        self.data_manager.update_document_status(update_doc_id, {
                            'conflict_analysis_status': 'Analyzing',
                            'conflict_status': 'Analyzing', 
                            'last_conflict_check': None,
                            'modified_date': datetime.now().isoformat()
                        })
                    except Exception as update_error:
                        logger.error(f"Error updating status: {str(update_error)}")
                
                self._analysis_start_time = datetime.now()
                
                if not hasattr(self, 'conflict_manager'):
                    self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                        
                max_analysis_time = 120  # 2 phút
                
                def analyze_in_background():
                    try:
                        import signal
                        import threading
                        
                        def timeout_handler():
                            logger.warning(f"Analysis timeout for document {doc_id}")
                            for update_doc_id in docs_to_update:
                                try:
                                    self.data_manager.update_document_status(update_doc_id, {
                                        'conflict_analysis_status': 'AnalysisFailed',
                                        'conflict_status': 'No Conflict',
                                        'conflict_analysis_error': 'Analysis timed out'
                                    })
                                except:
                                    pass
                                    
                            try:
                                self._analysis_timed_out = True
                                self._analysis_doc_id = doc_id
                            except Exception as e:
                                logger.error(f"Error setting timeout flags: {str(e)}")
                        
                        timer = threading.Timer(max_analysis_time, timeout_handler)
                        timer.daemon = True
                        timer.start()
                        
                        try:
                            success_count = 0
                            for analyze_doc_id in docs_to_update:
                                try:
                                    logger.info(f"Analyzing document {analyze_doc_id}")
                                    self.conflict_manager.analyze_document(analyze_doc_id)
                                    success_count += 1
                                except Exception as doc_error:
                                    error_msg = str(doc_error).replace('%', '%%')
                                    logger.error(f"Error analyzing document {analyze_doc_id}: {error_msg}")
                            
                            if duplicate_group_id and success_count > 0:
                                self.conflict_manager.sync_group_conflicts_by_group(duplicate_group_id)
                            
                            timer.cancel()
                        except Exception as analyze_error:
                            logger.error(f"Error in analysis: {str(analyze_error)}")
                            timer.cancel()
                        
                        try:
                            self._analysis_completed = True
                            self._analysis_doc_id = doc_id
                            self._analysis_time = (datetime.now() - self._analysis_start_time).total_seconds()
                            
                            self.data_manager.update_document_status(doc_id, {
                                'conflict_analysis_status': 'Analyzed',
                                'last_conflict_check': datetime.now().isoformat()
                            })
                        except Exception as update_error:
                            logger.error(f"Error updating completion status: {str(update_error)}")
                        
                    finally:
                        self._analysis_in_progress = False
                        if hasattr(self, '_analysis_mutex'):
                            try:
                                self._analysis_mutex.release()
                            except RuntimeError:
                                pass
                
                
                import threading
                analysis_thread = threading.Thread(target=analyze_in_background, daemon=True)
                analysis_thread.start()
                
                self.show_notification("Đang phân tích mâu thuẫn...", alert_type="info")
                return True
                    
            except Exception as e:
                logger.error(f"Error requesting conflict analysis: {str(e)}")
                logger.error(traceback.format_exc())
                self.show_notification(f"Lỗi: {str(e)}", alert_type="error")
                
                self._analysis_in_progress = False
                return False
                
            finally:
                if hasattr(self, '_analysis_mutex'):
                    try:
                        self._analysis_mutex.release()
                    except RuntimeError:
                        pass
        except Exception as outer_e:
            logger.error(f"Outer error in conflict analysis: {str(outer_e)}")
            return False
    
    
    def check_chunk_status(self, force_check=False):
        try:
            if not force_check and ((hasattr(self, '_user_is_interacting') and self._user_is_interacting) or \
            (hasattr(self, '_pause_auto_updates') and self._pause_auto_updates)):
                return
                
            if not self.current_doc_id:
                return
                        
            if (self.tabs.active != 1 and self.tabs.active != 3 and not force_check):
                return
                        
            if hasattr(self, '_is_loading_chunks') and self._is_loading_chunks:
                return
            
            current_time = time.time()
            last_check = getattr(self, '_last_chunk_status_check', 0)
            
            if current_time - last_check < 5 and not force_check:  
                return
                        
            self._last_chunk_status_check = current_time
                        
            document = self.data_manager.get_document_by_id(self.current_doc_id)
            if not document:
                return
                        
            current_status = document.get('chunk_status')
            cached_status = self._chunk_status_cache.get(self.current_doc_id)
            
            conflict_analysis_status = document.get('conflict_analysis_status', 'NotAnalyzed')
            conflict_status = document.get('conflict_status', 'No Conflict')
            has_conflicts = document.get('has_conflicts', False)
            
            logger.info(f"Check status: {self.current_doc_id} - chunk_status: {current_status}, " 
                    f"conflict_status: {conflict_status}, "
                    f"has_conflicts: {has_conflicts}, "
                    f"conflict_analysis_status: {conflict_analysis_status}")
                    
            tab_viewing = getattr(self, '_currently_viewing_tab', None)
            if tab_viewing is None:
                self._currently_viewing_tab = self.tabs.active
            
            if self.tabs.active == 3:
                recent_conflict_load = hasattr(self, '_last_conflict_load_time') and \
                    current_time - self._last_conflict_load_time < 10
                    
                if recent_conflict_load and not force_check:
                    return
                    
                if (conflict_analysis_status == 'Analyzing' or conflict_status == 'Analyzing'):
                    modified_date = document.get('modified_date')
                    analyzing_time_seconds = 0
                    
                    if modified_date:
                        try:
                            if isinstance(modified_date, str):
                                modified_date = datetime.fromisoformat(modified_date)
                                
                            analyzing_time_seconds = (datetime.now() - modified_date).total_seconds()
                            
                            if analyzing_time_seconds > 300:
                                logger.warning(f"Analysis has been running for too long ({analyzing_time_seconds}s), stopping it")
                                self._stop_infinite_analyzing(self.current_doc_id)
                                return
                        except Exception as date_error:
                            logger.error(f"Error parsing modified date: {str(date_error)}")
                    
                    if hasattr(self, '_conflict_reload_count') and self._conflict_reload_count >= 5:
                        logger.warning(f"Too many reload attempts ({self._conflict_reload_count}), stopping analysis")
                        self._stop_infinite_analyzing(self.current_doc_id)
                        return
                    
                    last_conflict_reload = getattr(self, '_last_conflict_reload', 0)
                    if current_time - last_conflict_reload > 10:  
                        if not hasattr(self, '_conflict_reload_count'):
                            self._conflict_reload_count = 0
                            
                        self._conflict_reload_count += 1
                        self._last_conflict_reload = current_time
                        
                        logger.info(f"Analysis in progress, reloading conflicts data (attempt {self._conflict_reload_count})")
                        
                        if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                            self.load_conflicts_data(self.current_doc_id)
                
                elif (conflict_analysis_status == 'Analyzed' and 
                    conflict_status not in ['Analyzing', 'NotAnalyzed'] and
                    not hasattr(self, '_conflicts_ui_created')):
                    
                    logger.info(f"Analysis completed, loading conflicts data for {self.current_doc_id}")
                    
                    if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                        self.load_conflicts_data(self.current_doc_id) 
                    
                    if hasattr(self, '_conflict_reload_count'):
                        self._conflict_reload_count = 0
                
                elif conflict_analysis_status == 'AnalysisFailed':
                    if not hasattr(self, '_conflicts_error_shown') or not self._conflicts_error_shown:
                        self._conflicts_error_shown = True
                        self.show_notification("Phân tích mâu thuẫn thất bại. Vui lòng thử lại.", alert_type="error")
                        
                        if (not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts) and \
                        (not hasattr(self, '_conflicts_ui_created') or not self._conflicts_ui_created):
                            self.load_conflicts_data(self.current_doc_id)
            
            if ((cached_status in ['Pending', 'Processing', 'Chunking'] and current_status == 'Chunked')
                or force_check):
                
                self._chunk_status_cache[self.current_doc_id] = current_status
                        
                if self.tabs.active == 1 and not self._is_loading_chunks:
                    recent_load = hasattr(self, '_last_chunks_load_time') and \
                        current_time - self._last_chunks_load_time < 10
                    if not recent_load:
                        self.load_chunks_data(self.current_doc_id)
            
            if self.tabs.active == 3 and self._check_conflict_update_needed() and not recent_conflict_load:
                if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                    logger.info(f"Updating conflict info for document {self.current_doc_id}")
                    self._last_conflict_load_time = current_time
                    self.load_conflicts_data(self.current_doc_id)
                        
        except Exception as e:
            logger.error(f"Error checking chunk status: {str(e)}")
            logger.error(traceback.format_exc())
        
    def _display_cached_conflicts(self, conflict_info, doc_id):
        try:
            if not hasattr(self, 'conflicts_container') or self.conflicts_container is None:
                self.conflicts_container = pn.Column(sizing_mode='stretch_width')
            
            self.conflicts_container.clear()
            
            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                self.conflicts_container.append(
                    pn.pane.Markdown(
                        "### Tài liệu không tồn tại hoặc đã bị xóa",
                        styles={
                            'color': '#E53E3E',
                            'text-align': 'center',
                            'padding': '15px',
                            'background': '#FFF5F5',
                            'border-radius': '8px',
                            'margin': '10px 0'
                        }
                    )
                )
                return
            
            content_conflicts = conflict_info.get('content_conflicts', [])
            internal_conflicts = conflict_info.get('internal_conflicts', [])
            external_conflicts = conflict_info.get('external_conflicts', [])
            
            total_content_conflicts = 0
            for conflict in content_conflicts:
                if isinstance(conflict, dict):
                    if "contradictions" in conflict:
                        contradictions = conflict.get("contradictions", [])
                        total_content_conflicts += len(contradictions) if contradictions else 1
                    else:
                        total_content_conflicts += 1
                else:
                    total_content_conflicts += 1
            
            conflict_counts = {
                'content': total_content_conflicts,
                'internal': len(internal_conflicts),
                'external': len(external_conflicts),
                'total': total_content_conflicts + len(internal_conflicts) + len(external_conflicts)
            }
            
            logger.info(f"Displaying conflicts: content={conflict_counts['content']}, "
                    f"internal={conflict_counts['internal']}, external={conflict_counts['external']}, "
                    f"total={conflict_counts['total']}")
            
            has_conflicts = conflict_counts['total'] > 0
            conflict_status = document.get('conflict_status', 'No Conflict')
            conflict_analysis_status = document.get('conflict_analysis_status', 'NotAnalyzed')
            last_check = document.get('last_conflict_check', "Chưa kiểm tra")
            
            if last_check and last_check != "Chưa kiểm tra":
                if isinstance(last_check, str):
                    try:
                        last_check = datetime.fromisoformat(last_check)
                        formatted_time = format_date(last_check)
                    except ValueError:
                        formatted_time = last_check
                else:
                    formatted_time = str(last_check)
            else:
                formatted_time = "Chưa kiểm tra"
            
            analysis_status_display = "Đã phân tích" if conflict_analysis_status == "Analyzed" else conflict_analysis_status
            
            status_info = pn.pane.Markdown(
                "**Đang hiển thị dữ liệu từ bộ nhớ đệm để cải thiện hiệu suất.**\n\nNhấn nút 'Phân tích mâu thuẫn' để tải lại dữ liệu mới nhất.",
                styles={
                    'color': '#2563eb',
                    'background': '#dbeafe',
                    'padding': '10px',
                    'border-radius': '4px',
                    'margin': '10px 0',
                    'font-size': '10px'
                }
            )
                    
            if has_conflicts:
                status_display = "Có mâu thuẫn"
                if conflict_status in ["Pending Review", "Resolving", "Conflict"]:
                    status_display = "Có mâu thuẫn cần xem xét"
                
                conflict_summary_text = f"""### Trạng thái: {status_display}

                    Tổng số mâu thuẫn phát hiện: {conflict_counts['total']}
                    - Mâu thuẫn nội dung: {conflict_counts['content']}
                    - Mâu thuẫn nội bộ: {conflict_counts['internal']}
                    - Mâu thuẫn ngoại bộ: {conflict_counts['external']}

                    Lần kiểm tra cuối: {formatted_time}"""
                
                summary_styles = {
                    'background': '#fef2f2',
                    'padding': '10px',
                    'border-radius': '4px',
                    'border': '1px solid #fecaca',
                    'margin-bottom': '10px'
                }
                
            else:
                status_display = "Không có mâu thuẫn"
                if conflict_status not in ["No Conflict", "Không mâu thuẫn"]:
                    status_display = conflict_status
                        
                conflict_summary_text = f"""### Trạng thái: {status_display}

                    **Không phát hiện xung đột nào trong tài liệu này.**

                    *Lần kiểm tra cuối: {formatted_time}*"""
                
                summary_styles = {
                    'background': '#f0fdf4',
                    'padding': '10px',
                    'border-radius': '4px',
                    'border': '1px solid #bbf7d0',
                    'margin-bottom': '10px'
                }
            
            conflict_summary = pn.pane.Markdown(
                conflict_summary_text,
                styles=summary_styles
            )
            
            reload_button = pn.widgets.Button(
                name="🔍 Phân tích mâu thuẫn",
                button_type="primary",
                button_style="solid",
                width=180,
                height=40,
                styles={
                    'font-weight': 'bold',
                    'font-size': '14px',
                    'box-shadow': '0 2px 4px rgba(0,0,0,0.1)',
                    'background-color': '#1e78c8',
                    'border-color': '#1e78c8'
                }
            )
            reload_button.on_click(lambda event: self.request_conflict_analysis(doc_id))
            
            model_status = pn.pane.Markdown(
                "**Model phân tích:** OpenAI",
                styles={
                    'font-size': '12px',
                    'color': '#4a5568',
                    'padding': '5px 10px',
                    'background': '#edf2f7',
                    'border-radius': '4px',
                    'display': 'inline-block'
                }
            )
            
            self.conflicts_container.append(
                pn.Row(
                    pn.pane.Markdown("### Thông tin mâu thuẫn", styles={
                        'color': '#2c5282',
                        'font-size': '18px',
                        'margin-bottom': '5px'
                    }),
                    pn.layout.HSpacer(),
                    model_status,
                    pn.Spacer(width=20),
                    reload_button,
                    align='center',
                    sizing_mode='stretch_width'
                )
            )
            
            self.conflicts_container.append(status_info)
            self.conflicts_container.append(conflict_summary)
            
            content_conflicts_container = pn.Column(name="Mâu thuẫn nội dung")
            internal_conflicts_container = pn.Column(name="Mâu thuẫn nội bộ")
            external_conflicts_container = pn.Column(name="Mâu thuẫn ngoại bộ")
            
            if conflict_counts['content'] > 0:
                content_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['content']} mâu thuẫn nội dung chunk**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '12px',
                            'border-radius': '6px',
                            'border-left': '4px solid #2563eb'
                        }
                    )
                )
                
                displayed_conflicts = 0
                max_conflicts_to_display = 10
                
                for conflict in content_conflicts:
                    if displayed_conflicts >= max_conflicts_to_display:
                        break
                        
                    if isinstance(conflict, dict):
                        if "contradictions" in conflict:
                            contradictions = conflict.get("contradictions", [])
                            for contradiction in contradictions:
                                if displayed_conflicts >= max_conflicts_to_display:
                                    break
                                contradiction_card = self._create_single_contradiction_card(
                                    contradiction,
                                    conflict.get("chunk_ids", []),
                                    conflict.get("analyzed_at", "")
                                )
                                content_conflicts_container.append(contradiction_card)
                                displayed_conflicts += 1
                        else:
                            conflict_card = self._create_conflict_card(conflict, "content")
                            content_conflicts_container.append(conflict_card)
                            displayed_conflicts += 1
                    else:
                        logger.warning(f"Invalid content conflict format: {type(conflict)}")
                        
                if conflict_counts['content'] > displayed_conflicts:
                    content_conflicts_container.append(
                        pn.pane.Markdown(
                            f"**...và {conflict_counts['content'] - displayed_conflicts} mâu thuẫn khác. Nhấn 'Phân tích mâu thuẫn' để xem đầy đủ.**",
                            styles={
                                'color': '#4b5563',
                                'font-size': '14px',
                                'margin': '10px 0',
                                'text-align': 'center'
                            }
                        )
                    )
            else:
                content_conflicts_container.append(
                    self._create_no_conflicts_message("nội dung")
                )
                
            if conflict_counts['internal'] > 0:
                internal_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['internal']} mâu thuẫn nội bộ**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '12px',
                            'border-radius': '6px',
                            'border-left': '4px solid #2563eb'
                        }
                    )
                )
                for i, conflict in enumerate(internal_conflicts[:10]):
                    conflict_card = self._create_conflict_card(conflict, "internal")
                    internal_conflicts_container.append(conflict_card)
                    
                if len(internal_conflicts) > 10:
                    internal_conflicts_container.append(
                        pn.pane.Markdown(
                            f"**...và {len(internal_conflicts) - 10} mâu thuẫn khác. Nhấn 'Phân tích mâu thuẫn' để xem đầy đủ.**",
                            styles={
                                'color': '#4b5563',
                                'font-size': '14px',
                                'margin': '10px 0',
                                'text-align': 'center'
                            }
                        )
                    )
            else:
                internal_conflicts_container.append(
                    self._create_no_conflicts_message("nội bộ")
                )
                
            if conflict_counts['external'] > 0:
                external_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['external']} mâu thuẫn ngoại bộ**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '12px',
                            'border-radius': '6px',
                            'border-left': '4px solid #2563eb'
                        }
                    )
                )
                for i, conflict in enumerate(external_conflicts[:10]):
                    conflict_card = self._create_conflict_card(conflict, "external")
                    external_conflicts_container.append(conflict_card)
                    
                if len(external_conflicts) > 10:
                    external_conflicts_container.append(
                        pn.pane.Markdown(
                            f"**...và {len(external_conflicts) - 10} mâu thuẫn khác. Nhấn 'Phân tích mâu thuẫn' để xem đầy đủ.**",
                            styles={
                                'color': '#4b5563',
                                'font-size': '14px',
                                'margin': '10px 0',
                                'text-align': 'center'
                            }
                        )
                    )
            else:
                external_conflicts_container.append(
                    self._create_no_conflicts_message("ngoại bộ")
                )
            
            conflict_tabs = pn.Tabs(
                ("Mâu thuẫn nội dung", content_conflicts_container),
                ("Mâu thuẫn nội bộ", internal_conflicts_container),
                ("Mâu thuẫn ngoại bộ", external_conflicts_container),
                sizing_mode='stretch_width'
            )
            
            self.conflicts_container.append(conflict_tabs)
                
            total_conflicts = conflict_counts['total']
            if total_conflicts > 0:
                summary = f"Tổng số mâu thuẫn: {total_conflicts} "
                summary += f"(Nội dung: {conflict_counts['content']}, Nội bộ: {conflict_counts['internal']}, Ngoại bộ: {conflict_counts['external']})"
                self.show_info_message(summary)
            else:
                self.show_info_message("Không phát hiện xung đột trong tài liệu này")
            
            if conflict_analysis_status == 'NotAnalyzed' and 'conflict_info' not in document:
                notice = pn.pane.Markdown(
                    "### Tài liệu này chưa được phân tích xung đột\n\n"
                    "Tài liệu này có thể đã bỏ qua bước phân tích xung đột trong quá trình xử lý."
                    "Vui lòng nhấp vào nút 'Phân tích mâu thuẫn' để thực hiện phân tích.",
                    styles={
                        'background': '#fff7ed',
                        'padding': '15px',
                        'border-radius': '8px',
                        'border': '1px solid #fed7aa',
                        'margin': '20px 0',
                        'color': '#9a3412'
                    }
                )
                analyze_button = pn.widgets.Button(
                    name="Phân tích mâu thuẫn",
                    button_type="primary",
                    button_style="solid",
                    width=180,
                    height=40,
                    styles={
                        'font-weight': 'bold',
                        'font-size': '14px',
                        'box-shadow': '0 2px 4px rgba(0,0,0,0.1)',
                        'background-color': '#1e78c8',
                        'border-color': '#1e78c8'
                    }
                )
                analyze_button.on_click(lambda event: self.request_conflict_analysis(doc_id))
                
                self.conflicts_container.append(
                    pn.Column(
                        notice,
                        pn.Row(analyze_button, align='center')
                    )
                )
            
            self._conflicts_ui_created = True

            try:
                if document.get('needs_conflict_reanalysis'):
                    self.data_manager.update_document_status(doc_id, {
                        'needs_conflict_reanalysis': False
                    })
                    logger.info(f"Reset needs_conflict_reanalysis flag for document {doc_id}")
            except Exception as reset_error:
                logger.error(f"Error resetting reanalysis flag: {str(reset_error)}")

            self._conflict_reload_count = 0

        except Exception as e:
            logger.error(f"Error loading conflict information: {str(e)}")
            logger.error(traceback.format_exc())
            self.show_error_message("Lỗi khi tải thông tin mâu thuẫn")
            
            if hasattr(self, 'conflicts_container'):
                self.conflicts_container.clear()
                self.conflicts_container.append(
                    pn.pane.Markdown(
                        f"### ❌ Lỗi khi tải thông tin mâu thuẫn\n{str(e)}",
                        styles={
                            'color': '#e53e3e',
                            'background': '#fff5f5',
                            'padding': '15px',
                            'border-radius': '8px',
                            'border': '1px solid #feb2b2',
                            'margin': '10px 0'
                        }
                    )
                )

        finally:
            if hasattr(self, 'approve_button'):
                self.approve_button.visible = True
            if hasattr(self, 'reject_button'):
                self.reject_button.visible = True
            if hasattr(self, 'delete_button'):
                self.delete_button.visible = True
            self._is_loading_conflicts = False
    
    def setup_conflict_analysis_monitoring(self, period=2000):
        """
        Set up conflict analysis progress tracking

        Args:
        period (int): Check period (ms)
        """
        try:
            logger.info(f"Setting up conflict analysis monitoring every {period}ms")
            
            if hasattr(self, 'conflict_monitoring_callback') and self.conflict_monitoring_callback is not None:
                try:
                    self.conflict_monitoring_callback.stop()
                except Exception:
                    pass
                
            self._analysis_completed = False
            self._analysis_timed_out = False
            self._analysis_doc_id = None
            self._analysis_time = 0
            
            def check_analysis_state():
                """Check both regular conflict progress and analysis completion flags"""
                try:
                    self.monitor_conflict_analysis()
                    
                    if hasattr(self, '_analysis_completed') and self._analysis_completed:
                        doc_id = getattr(self, '_analysis_doc_id', None)
                        if doc_id and doc_id == self.current_doc_id:
                            analysis_time = getattr(self, '_analysis_time', 0)
                            
                            if hasattr(self, 'tabs') and self.tabs.active == 3:
                                if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                                    self.load_conflicts_data(doc_id)
                            
                            self.show_notification(
                                f"Phân tích mâu thuẫn hoàn tất trong {analysis_time:.1f} giây",
                                alert_type="success"
                            )
                        
                        self._analysis_completed = False
                        self._analysis_doc_id = None
                        
                    if hasattr(self, '_analysis_timed_out') and self._analysis_timed_out:
                        doc_id = getattr(self, '_analysis_doc_id', None)
                        if doc_id and doc_id == self.current_doc_id:
                            if hasattr(self, 'tabs') and self.tabs.active == 3:
                                if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                                    self.load_conflicts_data(doc_id)
                            
                            self.show_notification(
                                "Phân tích mâu thuẫn đã hết thời gian chờ",
                                alert_type="error",
                                duration=5000
                            )
                        
                        self._analysis_timed_out = False
                        self._analysis_doc_id = None
                    
                except Exception as e:
                    logger.error(f"Error in conflict analysis monitoring: {str(e)}")
            
            try:
                if hasattr(pn.state, 'add_periodic_callback'):
                    self.conflict_monitoring_callback = pn.state.add_periodic_callback(
                        check_analysis_state,
                        period
                    )
                elif hasattr(pn, 'callbacks') and hasattr(pn.callbacks, 'periodic'):
                    self.conflict_monitoring_callback = pn.callbacks.periodic(
                        period, 
                        check_analysis_state
                    )
                else:
                    logger.warning("Panel periodic callbacks not available, using threading for monitoring")
                    import threading
                    
                    def repeating_monitor():
                        check_analysis_state()
                        
                        if hasattr(self, '_monitoring_active') and self._monitoring_active:
                            threading.Timer(period/1000, repeating_monitor).start()
                    
                    self._monitoring_active = True
                    monitoring_thread = threading.Timer(period/1000, repeating_monitor)
                    monitoring_thread.daemon = True
                    monitoring_thread.start()
                    
                    self.conflict_monitoring_callback = monitoring_thread
                    
            except Exception as cb_error:
                logger.error(f"Could not set up conflict monitoring: {str(cb_error)}")
            
        except Exception as e:
            logger.error(f"Error setting up conflict analysis monitoring: {str(e)}")
            logger.error(traceback.format_exc())
        
    def monitor_conflict_analysis(self):
        """
        Track conflict analysis progress and automatically update UI with better coordination
        """
        try:
            if not self.current_doc_id:
                return
                
            if not hasattr(self, 'tabs') or self.tabs.active != 3:
                return
                
            if hasattr(self, '_is_monitoring_conflicts') and self._is_monitoring_conflicts:
                return
                
            self._is_monitoring_conflicts = True
            
            try:
                document = self.data_manager.get_document_by_id(self.current_doc_id)
                if not document:
                    self._is_monitoring_conflicts = False
                    return
                    
                conflict_analysis_status = document.get('conflict_analysis_status', 'NotAnalyzed')
                conflict_status = document.get('conflict_status', 'No Conflict')
                last_check = document.get('last_conflict_check')
                last_modified = document.get('modified_date')
                
                if conflict_analysis_status == 'Analyzed' and conflict_status != 'Analyzing':
                    is_recently_completed = False
                    
                    if last_modified and last_check:
                        try:
                            if isinstance(last_modified, str):
                                last_modified = datetime.fromisoformat(last_modified)
                            if isinstance(last_check, str):
                                last_check = datetime.fromisoformat(last_check)
                                
                            if (datetime.now() - last_check).total_seconds() < 10:
                                is_recently_completed = True
                        except Exception as date_error:
                            logger.error(f"Error parsing dates: {str(date_error)}")
                    
                    if is_recently_completed and (not hasattr(self, '_conflicts_ui_created') or not self._conflicts_ui_created):
                        logger.info(f"Conflict analysis recently completed, auto-loading results")
                        
                        if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                            self.load_conflicts_data(self.current_doc_id)
                            self.show_notification("Phân tích mâu thuẫn đã hoàn tất", alert_type="success")
                
                elif conflict_analysis_status == 'Analyzing' or conflict_status == 'Analyzing':
                    if hasattr(self, '_conflict_reload_count') and self._conflict_reload_count >= 5:
                        logger.warning(f"Too many reload attempts ({self._conflict_reload_count}), not reloading")
                        self._is_monitoring_conflicts = False
                        return
                    
                    modified_date = document.get('modified_date')
                    if modified_date:
                        try:
                            if isinstance(modified_date, str):
                                modified_date = datetime.fromisoformat(modified_date)
                                
                            analyzing_time_seconds = (datetime.now() - modified_date).total_seconds()
                            
                            if analyzing_time_seconds > 300:
                                logger.warning(f"Analysis has been running for too long ({analyzing_time_seconds}s), stopping it")
                                self._stop_infinite_analyzing(self.current_doc_id)
                                self._is_monitoring_conflicts = False
                                return
                        except Exception as date_error:
                            logger.error(f"Error parsing modified date: {str(date_error)}")
                    
                    current_time = time.time()
                    last_reload = getattr(self, '_last_analysis_reload', 0)
                    
                    if current_time - last_reload > 7:  
                        self._last_analysis_reload = current_time
                        
                        if hasattr(self, '_conflict_reload_count'):
                            self._conflict_reload_count += 1
                        else:
                            self._conflict_reload_count = 1
                            
                        logger.info(f"Analysis in progress, auto-reloading conflicts data (attempt {self._conflict_reload_count})")
                        
                        if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                            self.load_conflicts_data(self.current_doc_id)
                
                elif conflict_analysis_status == 'AnalysisFailed':
                    if not hasattr(self, '_analysis_error_shown'):
                        self._analysis_error_shown = True
                        error_msg = document.get('conflict_analysis_error', 'Không rõ lỗi')
                        self.show_notification(f"Phân tích mâu thuẫn thất bại: {error_msg}", alert_type="error")
                        
                        if (not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts) and \
                        (not hasattr(self, '_conflicts_ui_created') or not self._conflicts_ui_created):
                            self.load_conflicts_data(self.current_doc_id)
                        
            except Exception as monitor_error:
                logger.error(f"Error in conflict monitoring: {str(monitor_error)}")
                
            finally:
                self._is_monitoring_conflicts = False
                
        except Exception as e:
            logger.error(f"Error in monitor_conflict_analysis: {str(e)}")
            logger.error(traceback.format_exc())
            self._is_monitoring_conflicts = False
    
    def _stop_infinite_analyzing(self, doc_id):
        """
        Stop infinite analyzing state by updating analysis status and cleaning up resources
        """
        try:
            logger.info(f"Stopping infinite analyzing for document {doc_id}")
            
            self.data_manager.update_document_status(doc_id, {
                'conflict_analysis_status': 'AnalysisFailed',
                'conflict_status': 'No Conflict',
                'conflict_analysis_error': 'Phân tích bị hủy do quá thời gian'
            })
            
            import threading
            if hasattr(self, '_conflict_reload_lock') and isinstance(self._conflict_reload_lock, threading.Lock):
                self._conflict_reload_lock = threading.Lock() 
            
            self._analysis_in_progress = False
            self._conflict_reload_count = 0
            self._is_loading_conflicts = False
            self._last_conflict_reload = 0
            self._conflicts_ui_created = False
            self._infinite_analyze_warning_shown = True
            
            self.show_notification(
                "Phân tích mâu thuẫn đã bị hủy do quá thời gian. Vui lòng thử lại sau.", 
                alert_type="warning",
                duration=5000
            )
            
            if hasattr(self, 'conflicts_container'):
                self.conflicts_container.clear()
                timeout_message = pn.pane.Markdown(
                    "### Phân tích mâu thuẫn đã bị hủy do quá thời gian",
                    styles={
                        'color': '#E53E3E',
                        'text-align': 'center',
                        'padding': '15px',
                        'background': '#FFF5F5',
                        'border-radius': '8px',
                        'margin': '10px 0'
                    }
                )
                
                retry_button = pn.widgets.Button(
                    name="Thử phân tích lại",
                    button_type="primary",
                    width=160
                )
                retry_button.on_click(lambda event: self.request_conflict_analysis(doc_id))
                
                self.conflicts_container.append(timeout_message)
                self.conflicts_container.append(pn.Row(retry_button, align='center'))
            
            document = self.data_manager.get_document_by_id(doc_id)
            if document and document.get('duplicate_group_id'):
                duplicate_group_id = document.get('duplicate_group_id')
                try:
                    update_query = """
                        UPDATE documents
                        SET conflict_analysis_status = 'NotAnalyzed',
                            conflict_status = CASE WHEN has_conflicts = true THEN 'Pending Review' ELSE 'No Conflict' END
                        WHERE duplicate_group_id = %s AND id != %s
                    """
                    self.data_manager.execute_with_retry(update_query, (duplicate_group_id, doc_id), fetch=False)
                    logger.info(f"Reset analysis status for other documents in group {duplicate_group_id}")
                except Exception as group_error:
                    logger.error(f"Error updating other documents in group: {str(group_error)}")
            
            logger.info(f"Successfully stopped infinite analyzing for document {doc_id}")
                
        except Exception as e:
            logger.error(f"Error stopping infinite analyzing state: {str(e)}")
            logger.error(traceback.format_exc())

    def get_layout(self):
        return self.layout