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
import threading
import sys
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))  
from common.models import ConflictResult
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
            self._cache_size = 10 
            self._cache_access_time = {}  
            
            self._last_loaded_chunks = None  
            self._is_loading_chunks = False
            self._is_updating = False
            self._last_chunk_load_time = 0
            self._chunk_load_debounce = 1000 
            self._last_update = datetime.now()
            
            logger.info("Cache system initialized")
        except Exception as e:
            logger.error(f"Error initializing cache: {str(e)}")
            self._chunks_cache = {}
            self._document_cache = {}
    
    def _update_cache(self, key, value):
        """
        Update cache with LRU (Least Recently Used) mechanism

        Args:
        key (str): Cache key
        value (any): Value to cache
        """
        try:
            self._chunks_cache[key] = value
            self._cache_access_time[key] = time.time()
            
            if len(self._chunks_cache) > self._cache_size:
                oldest_key = min(self._cache_access_time, key=self._cache_access_time.get)
                if oldest_key in self._chunks_cache:
                    del self._chunks_cache[oldest_key]
                if oldest_key in self._cache_access_time:
                    del self._cache_access_time[oldest_key]
                    
                logger.info(f"Removed oldest cache item: {oldest_key}")
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

    def setup_auto_update(self, period=5000):  
        try:
            self._last_update = datetime.now() - timedelta(seconds=10)  
            self._is_updating = False
            
            if hasattr(self, 'update_callback') and self.update_callback is not None:
                try:
                    self.update_callback.stop()
                except Exception:
                    pass
                    
            if hasattr(self, 'chunks_callback') and self.chunks_callback is not None:
                try:
                    self.chunks_callback.stop()
                except Exception:
                    pass
                    
            if hasattr(self, 'reanalysis_check_callback') and self.reanalysis_check_callback is not None:
                try:
                    self.reanalysis_check_callback.stop()
                except Exception:
                    pass
                    
            try:
                self.chunks_callback = pn.state.add_periodic_callback(
                    self.check_chunk_status,
                    5000 
                )
            except Exception as e:
                try:
                    if hasattr(pn.state, 'onload'):
                        pn.state.onload(lambda: pn.state.add_periodic_callback(self.check_chunk_status, 5000))
                except:
                    pass
            
            try:
                self.reanalysis_check_callback = pn.state.add_periodic_callback(
                    self.check_reanalysis_needed,
                    10000 
                )
            except Exception as e:
                logger.error(f"Could not set reanalysis check callback: {str(e)}")
                try:
                    if hasattr(pn.state, 'onload'):
                        pn.state.onload(lambda: pn.state.add_periodic_callback(self.check_reanalysis_needed, 10000))
                except:
                    pass
            
            try:
                self.update_callback = pn.state.add_periodic_callback(
                    self._throttled_update,
                    period  
                )
            except Exception as e:
                logger.error(f"Could not set update callback: {str(e)}")
                
            self._throttled_update()
            
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
                    })
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
            
            if hasattr(self, '_is_updating') and self._is_updating:
                return
                
            elapsed_seconds = (current_time - self._last_update).total_seconds()
            if elapsed_seconds < 2:  
                logger.info(f"Chỉ mới {elapsed_seconds}s từ lần cập nhật cuối, bỏ qua")
                return
                
            self.update_table()
            
        except Exception as e:
            self._is_updating = False
            logger.error(traceback.format_exc())
            
            try:
                pn.state.add_periodic_callback(self.update_table, 5000, count=1)
            except Exception:
                pass
            
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
            width=120
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

        column_configs = [
            {'field': col, 'title': self.column_titles.get(col, col), 'width': self.column_widths[col]}
            for col in available_columns if col in self.column_widths
        ]

        column_titles = []
        for col in available_columns:
            column_titles.append(self.column_titles.get(col, col))
        
        self.data_table = pn.widgets.Tabulator(
            value=filtered_data,
            pagination='local',
            page_size=10, 
            selectable=1,
            header_filters=self.data_filters,
            height=400,
            min_width=1200,
            disabled=False,
            sizing_mode="stretch_width",
            show_index=False,
            text_align='left',
            theme='bootstrap5',
            theme_classes=['table-striped', 'table-bordered'],
            selection=[],
            configuration={
                'layout': 'fitColumns',  # Co giãn vừa khít bảng
                'columns': column_configs
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

    def create_layout(self):        
        menu_items = [
            ("Quản lý tri thức", "/kms_admin"),
            ("Đánh giá hệ thống", "/main")
        ]
        
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
        </style>
        """
        
        def create_menu():
            current_page = pn.state.location.pathname

            menu_buttons = []
            for name, link in menu_items:
                button_type = "primary" if current_page == link else "default"
                button = pn.widgets.Button(name=name, button_type=button_type, width=140, height=40)
                button.js_on_click(args={"url": link}, code="window.location.href = url;")
                menu_buttons.append(button)
            return pn.Row(*menu_buttons, css_classes=["menu-bar"])
        
        menu = create_menu()
        
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
                pn.Spacer(width=20),
                menu,
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
                    
            if event.new == self._last_active_tab:
                return
                    
            self._last_active_tab = event.new
            
            if event.new != 1: 
                self._last_chunk_status = None
                    
            self._is_loading_tab = True
            
            if event.new == 3 and self.current_doc_id:
                document = self.data_manager.get_document_by_id(self.current_doc_id)
                if document:
                    conflict_analysis_status = document.get('conflict_analysis_status', 'NotAnalyzed')
                    conflict_status = document.get('conflict_status', 'No Conflict')
                    
                    if conflict_analysis_status == 'Analyzing' or conflict_status == 'Analyzing':
                        self._last_conflict_reload = time.time() - 10 
                    
                    if document.get('duplicate_group_id'):
                        if not hasattr(self, 'conflict_manager'):
                            self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                                
                        self.conflict_manager.sync_group_conflicts(self.current_doc_id)
                        
            self.load_tab_data(event.new)
            self._is_loading_tab = False
                    
        except Exception as e:
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
        """
        Handle the event triggered when the selection changes in the data table.

        This method is called whenever the user selects a new row in the data table. 
        It performs the following actions:
        - If no selection is made or the selection is invalid, it clears the document details.
        - If the selected document is the same as the current document (`self.current_doc_id`), no action is taken.
        - Otherwise, it updates the `current_doc_id` to the new selected document's ID, logs the change, and ensures that the tabs are visible.
        - It then loads data for the currently active tab.

        Parameters:
            event (object): The event object triggered by the selection change. This is typically
                            provided by the UI framework.

        Returns:
            None
        """
        try:
            selection = self.data_table.selection
            if not selection or len(self.all_data) <= selection[0]:
                self.update_button_states(None)  
                self.clear_detail_view()
                return
                    
            selected_row = self.all_data.iloc[selection[0]]
            doc_id = selected_row['id']
            
            self.update_button_states(selected_row)
            
            if doc_id != self.current_doc_id:
                self.current_doc_id = doc_id
                self.tabs.visible = True
                self.load_tab_data(self.tabs.active)
            
        except Exception as e:
            logger.error(f"Error in selection change: {str(e)}")
            self.clear_detail_view()
            self.current_doc_id = None
            self.update_button_states(None)  

    def extract_qa_pairs(self, qa_content: str) -> list:
        """
            Extract question and answer pairs from content in both Q/A and Hỏi/Đáp formats.
            
            This method extracts Q&A pairs from the content by normalizing formats,
            splitting into pairs, and cleaning whitespace.
            
            Args:
                qa_content (str): Raw content containing Q&A pairs
                    
            Returns:
                list: List of (question, answer) tuples
                    
            Raises:
                None: Method handles empty or invalid content by returning empty list
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
                     .replace('Trả lời:', 'A:'))
        

        # qa_content = qa_content.replace('**', '')

        if 'ORIGINAL TEXT:' in normalized_content:
            normalized_content = normalized_content.split('ORIGINAL TEXT:')[0]


        # Split on Q: and process each part
        parts = normalized_content.split('Q:')
        for part in parts[1:]:
            if 'A:' in part:
                q, a = part.split('A:', 1)
                q = q.strip() 
                a = a.strip()
                if q and a:
                    qa_pairs.append(('Q: ' + q, 'A: ' + a))

        return qa_pairs

    def load_chunks_data(self, doc_id):
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
                    pn.pane.Markdown(
                        "### Đang tải chunks...\nVui lòng chờ trong giây lát...",
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
            
            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                self.chunks_container.clear()
                self.show_error_message(f"Không tìm thấy thông tin tài liệu {doc_id}")
                self._is_loading_chunks = False
                return

            self._last_chunk_load_time = time.time() * 1000
            chunk_status = document.get('chunk_status')
            is_duplicate = document.get('is_duplicate')
            
            logger.info(f"Tải chunks cho tài liệu {doc_id}:")
            logger.info(f"- chunk_status: {chunk_status}")
            logger.info(f"- is_duplicate: {is_duplicate}")
            logger.info(f"- duplicate_group_id: {document.get('duplicate_group_id')}")
            logger.info(f"- original_chunked_doc: {document.get('original_chunked_doc')}")
            
            self._chunk_status_cache[doc_id] = chunk_status

            if is_duplicate:
                self._handle_referenced_chunks(document)
                return

            if chunk_status == 'NotRequired':
                self._handle_referenced_chunks(document)
                return
                
            if chunk_status in ['Pending', 'Chunking', 'Processing']:
                self._show_loading_state(chunk_status)
                self._is_loading_chunks = False
                return
                
            if chunk_status == 'ChunkingFailed':
                self._show_chunking_failed()
                self._is_loading_chunks = False
                return

            if chunk_status == 'Chunked':
                try:
                    retry_count = 0
                    max_retries = 3
                    
                    while retry_count < max_retries:
                        current_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id, limit=50)
                        
                        if not current_chunks and retry_count < max_retries - 1:
                            retry_count += 1
                            logger.info(f"Không tìm thấy chunks cho {doc_id}, thử lại lần {retry_count}/{max_retries}")
                            time.sleep(1)
                            continue
                        
                        self.chunks_container.clear()
                        
                        if not current_chunks:
                            logger.warning(f"No chunks found for document {doc_id} despite Chunked status")
                            self.show_error_message("Không tìm thấy chunks cho tài liệu này")
                            self._is_loading_chunks = False
                            return

                        cache_key = f"{doc_id}_chunks"
                        self._update_cache(cache_key, current_chunks)
                        self._last_loaded_chunks = doc_id

                        for chunk in current_chunks:
                            self._display_chunk(chunk, doc_id)

                        self.show_info_message(f"Đã tải {len(current_chunks)} chunks")
                        break  
                        
                except Exception as e:
                    logger.error(traceback.format_exc())
                    self.chunks_container.clear()
                    self.show_error_message(f"Lỗi khi tải chunks: {str(e)}")
            else:
                logger.warning(f"Unknown chunks status: {chunk_status}")
                self.show_error_message(f"Trạng thái chunks không hợp lệ: {chunk_status}")
                
        except Exception as e:
            logger.error(traceback.format_exc())
            self.show_error_message(f"Error loading chunks: {str(e)}")
            
        finally:
            self._is_loading_chunks = False
        
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
            
            
            if original_doc == doc_id:
                logger.warning(f"Detected document referencing itself: {doc_id}")
                original_doc = None
            
            if (not original_doc or original_doc == doc_id) and duplicate_group_id:
                logger.info(f"Find original document from group {duplicate_group_id}")
                group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                
                if group_docs:
                    chunked_docs = [d for d in group_docs if d.get('chunk_status') == 'Chunked']
                    if chunked_docs:
                        chunks_source = chunked_docs[0]['id']
                        logger.info(f"Found chunked documents in group: {chunks_source}")
                        
                        if chunks_source == doc_id:
                            is_duplicate = False
                            original_doc = None
                        else:
                            original_doc = chunks_source
                            is_duplicate = True
                    else:
                        original_candidates = [d for d in group_docs if not d.get('is_duplicate', False)]
                        if original_candidates:
                            original_candidates.sort(key=lambda d: d.get('created_date', ''))
                            original_doc_in_group = original_candidates[0]['id']
                            logger.info(f"Found original document in group: {original_doc_in_group}")
                            
                            if original_doc_in_group == doc_id:
                                is_duplicate = False
                                original_doc = None
                            else:
                                original_doc = original_doc_in_group
                                is_duplicate = True
                        else:
                            group_docs.sort(key=lambda d: d.get('created_date', ''))
                            earliest_doc = group_docs[0]['id']
                            
                            if earliest_doc == doc_id:
                                is_duplicate = False
                                original_doc = None
                            else:
                                original_doc = earliest_doc
                                is_duplicate = True
            
            is_original_document = (not is_duplicate) or (not original_doc or original_doc == doc_id)
                        
            if is_original_document:
                logger.info(f"Đây là tài liệu gốc: {doc_id}")
                try:
                    current_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
                    
                    if current_chunks:
                        self.chunks_container.clear()
                        for chunk in current_chunks:
                            self._display_chunk(chunk, doc_id)
                        self.show_info_message(f"Đã tải {len(current_chunks)} chunks")
                        return
                    
                    if duplicate_group_id:
                        logger.info(f"No direct chunks found, try searching from duplicate group {duplicate_group_id}")
                        group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                        
                        chunked_docs = [d for d in group_docs if d.get('chunk_status') == 'Chunked' and d['id'] != doc_id]
                        if chunked_docs:
                            alternate_source = chunked_docs[0]['id']
                            logger.info(f"Try loading chunks from another document in the group: {alternate_source}")
                            
                            alternate_chunks = self.chroma_manager.get_chunks_by_document_id(alternate_source)
                            if alternate_chunks:
                                self.chunks_container.clear()
                                self.chunks_container.append(
                                    pn.pane.Markdown(
                                        f"### Tải chunks từ tài liệu khác trong nhóm: {alternate_source}",
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
                                
                                for chunk in alternate_chunks:
                                    self._display_chunk(chunk, alternate_source)
                                    
                                self.show_info_message(f"Đã tải {len(alternate_chunks)} chunks từ tài liệu {alternate_source}")
                                return
                    
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
                            f"- Thuộc nhóm: {duplicate_group_id if duplicate_group_id else 'Không'}\n"
                            f"- Là tài liệu gốc: {'Có' if is_original_document else 'Không'}",
                            styles={
                                'color': '#4A5568',
                                'background': '#EDF2F7',
                                'padding': '15px',
                                'border-radius': '8px',
                                'margin-top': '15px'
                            }
                        )
                    )
                    
                    if chunk_status != 'Chunked':
                        self.chunks_container.append(
                            pn.pane.Markdown(
                                f"### Tài liệu chưa được xử lý chunks\n"
                                f"Trạng thái hiện tại: {chunk_status}",
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
                        
                    self.show_error_message("Không tìm thấy chunks cho tài liệu này")
                    
                except Exception as e:
                    logger.error(traceback.format_exc())
                    self.show_error_message(f"Lỗi khi tải chunks: {str(e)}")
                    
                self._is_loading_chunks = False
                return
                    
            if not original_doc or original_doc == doc_id:
                logger.warning(f"No original document found for {doc_id}")
                
                try:
                    direct_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
                    if direct_chunks:
                        self.chunks_container.clear()
                        self.chunks_container.append(
                            pn.pane.Markdown(
                                "### Tải trực tiếp chunks của tài liệu này",
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
                        
                        for chunk in direct_chunks:
                            self._display_chunk(chunk, doc_id)
                        self.show_info_message(f"Đã tải {len(direct_chunks)} chunks trực tiếp")
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
                        self.show_error_message("Không tìm thấy chunks")
                except Exception as e:
                    logger.error(f"Lỗi khi tải chunks trực tiếp: {str(e)}")
                    self.show_error_message(f"Lỗi khi tải chunks: {str(e)}")
                    
                self._is_loading_chunks = False
                return
                    
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
            else:
                self.chunks_container.clear()
            
            try:
                logger.info(f"Load chunks from original document {original_doc}")
                original_chunks = self.chroma_manager.get_chunks_by_document_id(original_doc)
                
                if original_chunks:
                    for chunk in original_chunks:
                        self._display_chunk(chunk, original_doc)
                    self.show_info_message(f"Loaded {len(original_chunks)} chunks from original document")
                else:
                    
                    logger.warning(f"Không tìm thấy chunks từ tài liệu gốc {original_doc}, thử tải từ tài liệu hiện tại")
          
                    try:
                        current_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
                        if current_chunks:
                            for chunk in current_chunks:
                                self._display_chunk(chunk, doc_id)
                            self.show_info_message(f"Đã tải {len(current_chunks)} chunks")
                        else:
                            self.chunks_container.append(
                                pn.pane.Markdown(
                                    "### Không tìm thấy chunks nào cho tài liệu này",
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
                    except Exception as current_error:
                        logger.error(f"Error loading chunks of current document: {str(current_error)}")
                                    
                    
            except Exception as e:
                logger.error(f"Error loading chunks from original document {original_doc}: {str(e)}")
                logger.error(traceback.format_exc())
                
                try:
                    current_chunks = self.chroma_manager.get_chunks_by_document_id(doc_id)
                    if current_chunks:
                        self.chunks_container.append(
                            pn.pane.Markdown(
                                "### Đang thử tải chunks của tài liệu hiện tại...",
                                styles={
                                    'color': '#3182CE',
                                    'text-align': 'center',
                                    'padding': '10px',
                                    'margin-top': '20px',
                                    'background': '#EBF8FF',
                                    'border-radius': '8px'
                                }
                            )
                        )
                        
                        for chunk in current_chunks:
                            self._display_chunk(chunk, doc_id)
                        self.show_info_message(f"Đã tải {len(current_chunks)} chunks của tài liệu hiện tại")
                    else:
                        self.chunks_container.append(
                            pn.pane.Markdown(
                                "### Không tìm thấy chunks nào cho tài liệu này",
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
                except Exception as fallback_error:
                    logger.error(f"Error loading fallback chunks: {str(fallback_error)}")
                    
                    self.chunks_container.append(
                        pn.pane.Markdown(
                            "### Không thể tải chunks cho tài liệu này",
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
                    
            finally:
                self._is_loading_chunks = False
                
        except Exception as e:
            logger.error(f"Lỗi khi xử lý tài liệu: {str(e)}")
            logger.error(traceback.format_exc())
            
            self.chunks_container.clear()
            self.chunks_container.append(
                pn.pane.Markdown(
                    "### Đã xảy ra lỗi khi tải chunks",
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
                                                            logger.error(f"Error updating conflicts UI: {str(ui_error)}")
                                                    
                                                    import threading
                                                    ui_thread = threading.Thread(target=update_conflicts_ui)
                                                    ui_thread.daemon = True
                                                    ui_thread.start()
                                        except Exception as analyze_error:
                                            logger.error(f"Error analyzing document {doc_id}: {str(analyze_error)}")
                                    
                                    if duplicate_group_id:
                                        for related_id in related_docs:
                                            if related_id != doc_id:
                                                try:
                                                    logger.info(f"Auto-analyzing related document {related_id}")
                                                    self.conflict_manager.analyze_document(related_id)
                                                except Exception as related_error:
                                                    logger.error(f"Error analyzing related document {related_id}: {str(related_error)}")
                                        
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

    def on_selection_change(self, event):
        """
        Handle changes in document selection within the data table.

        Returns:
            None
        """
        try:
            selection = self.data_table.selection
            if not selection or len(self.all_data) <= selection[0]:
                self.clear_detail_view()
                return
                
            selected_row = self.all_data.iloc[selection[0]]
            doc_id = selected_row['id']
            
            if doc_id != self.current_doc_id:
                self.current_doc_id = doc_id
                self.tabs.visible = True
                
                if hasattr(self, '_last_loaded_tab'):
                    del self._last_loaded_tab
                    
                self.load_tab_data(self.tabs.active)
            
        except Exception as e:
            logger.error(f"Error in selection change: {str(e)}")
            self.clear_detail_view()
            self.current_doc_id = None
        
    def on_row_click(self, event):
        """
        Handle the row click event in the data table.

        This method is triggered when a user clicks on a row in the data table. It sets the selected row 
        in the table and updates the `selection` state. The method ensures that the loading indicator 
        is shown while the selection is being processed. It does not load any data at this stage; it 
        only updates the selection state and handles any potential exceptions that may occur during 
        the process.

        Parameters:
            event (object): The event object that contains information about the row click, including 
                            the row index and any relevant event data.

        Returns:
            None: This method does not return any values. It updates the selection in the data table 
                and manages the loading indicator.
        """
        if event.row is not None:
            try:
                self.loading_indicator.value = True
                selected_row = self.all_data.iloc[event.row]
                
                self.update_button_states(selected_row)
                
                self.data_table.selection = [event.row]
                
            except Exception as e:
                logger.error(f"Lỗi khi click dòng: {str(e)}")
                self.clear_detail_view()
                self.current_doc_id = None
                self.update_button_states(None)
            finally:
                self.loading_indicator.value = False

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
            logger.info(f"Update table with filter: {status_filter}")
            
            try:
                if status_filter != "All":
                    if status_filter == "Pending":
                        self.all_data = self.data_manager.get_filtered_data(status="Pending")
                    elif status_filter == "Approved":
                        self.all_data = self.data_manager.get_filtered_data(status="Approved")
                    elif status_filter == "Rejected":
                        self.all_data = self.data_manager.get_filtered_data(status="Rejected")
                else:
                    self.all_data = self.data_manager.get_all_documents()
                        
            except Exception as db_error:
                logger.error(f"Error while getting data: {str(db_error)}")
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
                
                if 'conflict_status' in self.all_data.columns:
                    for idx, row in self.all_data.iterrows():
                        has_conflicts = row.get('has_conflicts', False)
                        conflict_status = row.get('conflict_status')
                        
                        if (has_conflicts and (conflict_status != "Mâu thuẫn")) or \
                        (not has_conflicts and (conflict_status == "Mâu thuẫn")):
                            self.all_data.at[idx, 'conflict_status'] = "Mâu thuẫn" if has_conflicts else "Không mâu thuẫn"
                
                self._format_initial_data()
                filtered_data = self.all_data[available_columns]
                self.data_table.value = filtered_data
                
                if self.current_doc_id and self.data_table.selection:
                    selected_index = self.data_table.selection[0]
                    if selected_index < len(self.all_data):
                        self.update_detail_view(selected_index)
                        
                if hasattr(self.data_table, 'param'):
                    self.data_table.param.trigger('value')
                        
            except Exception as ui_error:
                logger.error(traceback.format_exc())
                        
        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            self._is_updating = False
            if hasattr(self, 'loading_indicator'):
                self.loading_indicator.value = False
            self._last_update = datetime.now()
    
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

    def update_detail_view(self, selected_index=None):
        """
        Update view showing document details
        """
        try:
            if selected_index is None and self.data_table.selection:
                selected_index = self.data_table.selection[0]
                
            if selected_index is None or selected_index >= len(self.all_data):
                self.clear_detail_view()
                return

            selected_row = self.all_data.iloc[selected_index]
            content = selected_row.get('content', '')
            formatted_content = format_content_markdown(content)

            processed_content = ""
            if self.current_doc_id:
                try:
                    chunks = self.chroma_manager.get_chunks_by_document_id(self.current_doc_id)
                    if chunks:
                        qa_parts = []
                        for chunk in chunks:
                            qa_content = chunk.get('qa_content', '')
                            if qa_content:
                                qa_pairs = self.extract_qa_pairs(qa_content)
                                for q, a in qa_pairs:
                                    qa_parts.append(f"{q}\n{a}\n")
                        
                        processed_content = "\n".join(qa_parts)
                except Exception as e:
                    logger.error(f"Error loading QA content: {str(e)}")

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
                    'font-size': '15px',
                    'margin-bottom': '5px'
                }),
                pn.Row(
                    pn.Column(
                        pn.pane.Markdown(
                            f"**ID:** {selected_row.get('id', '')}\n"  
                            f"**Ngày tạo:** {selected_row.get('created_date', '')}",
                            styles={
                                'font-size': '13px',
                                'background': '#f8fafc',
                                'padding': '12px',
                                'border-radius': '6px', 
                                'border-left': '4px solid #4299e1',
                                'margin': '5px 0'
                            }
                        ),
                        pn.pane.Markdown(
                            "**Trạng thái:**",
                            styles={
                                'font-size': '13px',
                                'margin-bottom': '5px'
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
                                'font-size': '13px',
                                'background': '#f8fafc',
                                'padding': '12px', 
                                'border-radius': '6px',
                                'border-left': '4px solid #4299e1',
                                'margin': '5px 0'
                            }
                        ),
                        width=400
                    ),
                    sizing_mode='stretch_width'
                )
            )

            content_view = pn.Column(
                pn.pane.Markdown("### NỘI DUNG GỐC", styles={
                    'color': '#2c5282',
                    'font-size': '16px',
                    'font-weight': 'bold',
                    'margin-top': '20px',
                    'margin-bottom': '10px'
                }),
                pn.pane.Markdown(
                    formatted_content,
                    styles={
                        'background': '#f8fafc',
                        'padding': '15px',
                        'border-radius': '6px',
                        'font-size': '14px',
                        'line-height': '1.6',
                        'border-left': '4px solid #4299e1',
                        'white-space': 'pre-wrap',
                        'word-break': 'break-word'
                    }
                ),
                
                pn.pane.Markdown(
                    processed_content,
                    styles={
                        'background': '#f8fafc',
                        'padding': '15px',
                        'border-radius': '6px',
                        'font-size': '14px',
                        'line-height': '1.6',
                        'border-left': '4px solid #4299e1',
                        'white-space': 'pre-wrap',
                        'word-break': 'break-word'
                    }
                ),
                sizing_mode='stretch_width'
            )

            self.detail_view[:] = [
                general_info,
                content_view
            ]

            self.save_button.visible = True

        except Exception as e:
            logger.error(f"Error updating detail view: {str(e)}")
            traceback.print_exc()
            self.clear_detail_view()
        
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
                    'font-size': '16px',
                    'font-weight': 'bold'
                }),
                pn.Row(
                    pn.Column(
                        pn.pane.Markdown("Chưa có tài liệu được chọn", styles={
                            'font-style': 'italic',
                            'color': '#666'
                        }),
                        sizing_mode='stretch_width'
                    ),
                    sizing_mode='stretch_width'
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
                    self.data_table.value = empty_df
                else:
                    self.data_table.value = self.all_data[available_columns]
                    
                logger.info(f"Updated table with {len(self.data_table.value)} rows")
            except Exception as table_error:
                logger.error(f"Lỗi khi cập nhật bảng: {str(table_error)}")
                logger.error(traceback.format_exc())
            
            try:
                self.data_table.selection = []
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
                        analyzed_at = datetime.fromisoformat(analyzed_at)
                    formatted_time = analyzed_at.strftime('%d/%m/%Y %H:%M:%S')
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
                                        created_date = doc['created_date'].strftime('%d/%m/%Y %H:%M:%S')
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
            
            if analyzed_at:
                try:
                    if isinstance(analyzed_at, str):
                        analyzed_at = datetime.fromisoformat(analyzed_at)
                    formatted_time = analyzed_at.strftime('%d/%m/%Y %H:%M:%S')
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
        
        label = conflict_type_labels.get(conflict_type, conflict_type)
        
        return pn.pane.Markdown(
            f"**Không tìm thấy {label} nào**",
            styles={
                'color': '#047857',
                'font-size': '14px',
                'margin-top': '20px',
                'text-align': 'center',
                'background': '#ecfdf5',
                'padding': '20px',
                'border-radius': '8px',
                'border': '1px solid #a7f3d0',
                'box-shadow': '0 1px 2px rgba(0, 0, 0, 0.05)'
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
                            resolution_note = f"Tài liệu {doc_id} đã bị xóa vào {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                            
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
                                
                                # Lọc các mâu thuẫn nội dung
                                for conflict in conflict_info.get('content_conflicts', []):
                                    if isinstance(conflict, dict):
                                        chunk_id = conflict.get('chunk_id', '')
                                        if not chunk_id.startswith(doc_id):
                                            new_conflicts['content_conflicts'].append(conflict)
                                
                                # Lọc mâu thuẫn nội bộ
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
            
            self.show_notification("Đang tải lại mâu thuẫn...", alert_type="info")
            
            self.fix_conflicts_button = pn.widgets.Button(
                name="Tải lại mâu thuẫn", 
                button_type="primary",
                width=150
            )
            self.fix_conflicts_button.on_click(self.request_reload_conflicts)
            
            self.data_manager.fix_existing_external_conflicts()
            
            self.load_conflicts_data(self.current_doc_id)
            
            self.show_notification("Đã tải lại thông tin mâu thuẫn thành công", alert_type="success")
            
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
        Request conflict analysis for a document with better concurrency and error handling
        
        Args:
            doc_id (str): ID of the document to analyze
                
        Returns:
            bool: True if request successful, False if error
        """
        try:
            if not hasattr(self, '_analysis_mutex'):
                import threading
                self._analysis_mutex = threading.Lock()
                
            if not self._analysis_mutex.acquire(blocking=False):
                self.show_notification("Phân tích mâu thuẫn đang được thực hiện, vui lòng đợi", alert_type="warning")
                return False
                
            try:
                self._analysis_in_progress = True
                self._conflict_reload_count = 0
                self._conflicts_ui_created = False
                self._conflicts_error_shown = False
                self._infinite_analyze_warning_shown = False
                
                if not doc_id:
                    self.show_notification("Vui lòng chọn một tài liệu", alert_type="warning")
                    self._analysis_in_progress = False
                    return False
                        
                if hasattr(self, 'conflict_info_container') and self.conflict_info_container:
                    self.conflict_info_container.clear()
                    self.conflict_info_container.append(
                        pn.pane.Markdown("Yêu cầu phân tích mâu thuẫn...", styles={'color': 'blue'})
                    )
                    self.conflict_info_container.visible = True
                
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
                    return False
                        
                duplicate_group_id = document.get('duplicate_group_id')
                
                docs_to_update = [doc_id]
                if duplicate_group_id:
                    try:
                        group_docs = self.data_manager.get_documents_in_group(duplicate_group_id)
                        if group_docs:
                            docs_to_update = [doc['id'] for doc in group_docs]
                            logger.info(f"Will analyze all {len(docs_to_update)} documents in group {duplicate_group_id}")
                    except Exception as group_error:
                        logger.error(f"Error getting documents in group: {str(group_error)}")
                
                for update_doc_id in docs_to_update:
                    try:
                        self.data_manager.update_document_status(update_doc_id, {
                            'conflict_analysis_status': 'Analyzing',
                            'conflict_status': 'Analyzing', 
                            'last_conflict_check': None,
                            'modified_date': datetime.now().isoformat()  # Set current timestamp
                        })
                        logger.info(f"Updated conflict status for document {update_doc_id}")
                    except Exception as update_error:
                        logger.error(f"Error updating document {update_doc_id}: {str(update_error)}")
                
                self._analysis_start_time = datetime.now()
                
                conflict_api_url = os.getenv('CONFLICT_API_URL')
                
                if conflict_api_url:
                    try:
                        import requests
                        response = requests.post(
                            f"{conflict_api_url}/analyze/document",
                            json={"doc_id": doc_id, "priority": 1, "analyze_group": True},
                            timeout=5
                        )
                        
                        if response.status_code == 200:
                            result = response.json()
                            task_id = result.get('task_id')
                            self.show_notification(
                                f"Đã gửi yêu cầu phân tích mâu thuẫn", 
                                alert_type="info"
                            )
                            self._analysis_in_progress = False
                            return True
                        else:
                            error_msg = response.json().get('message', 'Unknown error')
                            logger.warning(f"API error: {error_msg}, falling back to local analysis")
                        
                            
                    except Exception as api_error:
                        logger.error(f"Error calling conflict analysis API: {str(api_error)}")
                        logger.info("Falling back to local conflict analysis")

                if not hasattr(self, 'conflict_manager'):
                    try:
                        self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                        logger.info("Created new ConflictManager instance")
                    except Exception as cm_error:
                        logger.error(f"Error creating ConflictManager: {str(cm_error)}")
                        self.show_notification("Error initializing conflict manager", alert_type="error")
                        self._analysis_in_progress = False
                        return False
                        
                def analyze_in_background():
                    total_analyzed = 0
                    failed_docs = []
                    
                    try:
                        for i, analyze_doc_id in enumerate(docs_to_update):
                            try:
                                logger.info(f"Starting analysis for document {analyze_doc_id} ({i+1}/{len(docs_to_update)})")
                                
                                try:
                                    self.data_manager.update_document_status(analyze_doc_id, {
                                        'modified_date': datetime.now().isoformat()
                                    })
                                except:
                                    pass
                                    
                                # Perform the analysis
                                conflict_info = self.conflict_manager.analyze_document(analyze_doc_id)
                                logger.info(f"Completed analysis for document {analyze_doc_id}")
                                total_analyzed += 1
                                
                            except Exception as doc_analyze_error:
                                logger.error(f"Error analyzing document {analyze_doc_id}: {str(doc_analyze_error)}")
                                logger.error(traceback.format_exc())
                                failed_docs.append(analyze_doc_id)
                                
                                try:
                                    self.data_manager.update_document_status(analyze_doc_id, {
                                        'conflict_analysis_status': 'AnalysisFailed',
                                        'conflict_status': 'No Conflict',
                                        'conflict_analysis_error': str(doc_analyze_error)[:500]  
                                    })
                                except:
                                    pass
                        
                        if len(docs_to_update) > 1 and total_analyzed > 0:
                            try:
                                logger.info(f"Syncing conflicts for group {duplicate_group_id}")
                                if duplicate_group_id:
                                    sync_result = self.conflict_manager.sync_group_conflicts_by_group(duplicate_group_id)
                                    logger.info(f"Group sync completed with result: {sync_result}")
                            except Exception as sync_error:
                                logger.error(f"Error during final group sync: {str(sync_error)}")
                        
                        def update_ui():
                            try:
                                self._analysis_in_progress = False
                                self._conflict_reload_count = 0
                                
                                if self.current_doc_id == doc_id:
                                    if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                                        self.load_conflicts_data(doc_id)
                                    
                                    if hasattr(self, 'tabs'):
                                        self.tabs.active = 3  
                                    
                                    analysis_duration = (datetime.now() - self._analysis_start_time).total_seconds()
                                    if failed_docs:
                                        self.show_notification(
                                            f"Phân tích hoàn tất với các vấn đề. {total_analyzed} tài liệu đã phân tích, {len(failed_docs)} không thành công.",
                                            alert_type="warning"
                                        )
                                    else:
                                        self.show_notification(
                                            f"Phân tích mâu thuẫn đã hoàn tất trong {analysis_duration:.1f} giây",
                                            alert_type="success"
                                        )
                                    
                                    document = self.data_manager.get_document_by_id(doc_id)
                                    if document and document.get('has_conflicts', False):
                                        if hasattr(self, 'tabs') and self.tabs.active != 3:
                                            self.tabs.active = 3
                                            self.show_notification(
                                                "Đã phát hiện mâu thuẫn, chuyển sang tab mâu thuẫn",
                                                alert_type="warning"
                                            )
                            except Exception as ui_error:
                                logger.error(f"Error updating UI after analysis: {str(ui_error)}")
                                self._analysis_in_progress = False
                        
                        import threading
                        update_thread = threading.Thread(target=update_ui, daemon=True)
                        update_thread.start()
                        
                    except Exception as analyze_error:
                        logger.error(f"Error in master analysis process: {str(analyze_error)}")
                        logger.error(traceback.format_exc())
                        
                        import threading
                        def show_error():
                            try:
                                self._analysis_in_progress = False
                                self.show_notification(f"Error: {str(analyze_error)}", alert_type="error")
                                for update_doc_id in docs_to_update:
                                    try:
                                        self.data_manager.update_document_status(update_doc_id, {
                                            'conflict_analysis_status': 'AnalysisFailed',
                                            'conflict_status': 'No Conflict',
                                            'conflict_analysis_error': str(analyze_error)[:500]
                                        })
                                    except:
                                        pass
                                self.load_conflicts_data(doc_id)
                            except Exception as error_show_error:
                                logger.error(f"Error showing error notification: {str(error_show_error)}")
                    
                    finally:
                        if hasattr(self, '_analysis_mutex'):
                            try:
                                self._analysis_mutex.release()
                                logger.info("Released analysis mutex")
                            except Exception:
                                pass
                
                import threading
                analysis_thread = threading.Thread(target=analyze_in_background, daemon=True)
                analysis_thread.start()
                
                self.show_notification("Đang phân tích mâu thuẫn...", alert_type="info")
                return True
                    
            except Exception as e:
                logger.error(f"Error requesting conflict analysis: {str(e)}")
                logger.error(traceback.format_exc())
                self.show_notification(f"Error: {str(e)}", alert_type="error")
                
                try:
                    if doc_id:
                        self.data_manager.update_document_status(doc_id, {
                            'conflict_analysis_status': 'NotAnalyzed',
                            'conflict_status': 'No Conflict'
                        })
                except:
                    pass
                
                self._analysis_in_progress = False
                return False
                
            finally:
                if hasattr(self, '_analysis_mutex'):
                    try:
                        self._analysis_mutex.release()
                        logger.info("Released analysis mutex")
                    except RuntimeError:
                        pass
        except Exception as outer_e:
            logger.error(f"Outer error in conflict analysis request: {str(outer_e)}")
            logger.error(traceback.format_exc())
            self.show_notification(f"Unexpected error: {str(outer_e)}", alert_type="error")
            return False

    def check_chunk_status(self, force_check=False):
        """
        Check and update chunk and conflict status with improved timeout handling
        """
        try:
            if not self.current_doc_id:
                return
                        
            if (self.tabs.active != 1 and self.tabs.active != 3 and not force_check):
                return
                        
            if hasattr(self, '_is_loading_chunks') and self._is_loading_chunks:
                return
            
            current_time = time.time()
            last_check = getattr(self, '_last_chunk_status_check', 0)
            
            if current_time - last_check < 3 and not force_check:  
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
                    
            if self.tabs.active == 3:
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
                    if current_time - last_conflict_reload > 7: 
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
                        
                        if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                            self.load_conflicts_data(self.current_doc_id)
            
            if ((cached_status in ['Pending', 'Processing', 'Chunking'] and current_status == 'Chunked')
                or force_check):
                
                self._chunk_status_cache[self.current_doc_id] = current_status
                        
                if self.tabs.active == 1:
                    self.load_chunks_data(self.current_doc_id)
                
            if self.tabs.active == 3 and self._check_conflict_update_needed():
                if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                    logger.info(f"Updating conflict info for document {self.current_doc_id}")
                    self.load_conflicts_data(self.current_doc_id)
                        
        except Exception as e:
            logger.error(f"Error checking chunk status: {str(e)}")
            logger.error(traceback.format_exc())
    
    def _display_cached_conflicts(self, conflict_info, doc_id):
        """
        Quickly display conflict information from cache

        Args:
        conflict_info (dict): Cache conflict information
        doc_id (str): ID of the document
        """
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
            
            conflict_counts = {
                'content': len(content_conflicts),
                'internal': len(internal_conflicts),
                'external': len(external_conflicts),
                'total': len(content_conflicts) + len(internal_conflicts) + len(external_conflicts)
            }
            
            has_conflicts = conflict_counts['total'] > 0
            conflict_status = document.get('conflict_status', 'No Conflict')
            last_check = document.get('last_conflict_check', "Chưa kiểm tra")
            
            if last_check and last_check != "Chưa kiểm tra":
                if isinstance(last_check, str):
                    try:
                        last_check = datetime.fromisoformat(last_check)
                        formatted_time = last_check.strftime('%d/%m/%Y %H:%M:%S')
                    except ValueError:
                        formatted_time = last_check
                else:
                    formatted_time = str(last_check)
            else:
                formatted_time = "Chưa kiểm tra"
            
            if has_conflicts:
                status_display = "Có mâu thuẫn"
                if conflict_status in ["Pending Review", "Resolving", "Conflict"]:
                    status_display = "Có mâu thuẫn cần xem xét"
                        
                summary_text = f"""### Trạng thái: {status_display}

                **Tổng số mâu thuẫn phát hiện: {conflict_counts['total']}**
                - Mâu thuẫn nội dung: {conflict_counts['content']}
                - Mâu thuẫn nội bộ: {conflict_counts['internal']}
                - Mâu thuẫn ngoại bộ: {conflict_counts['external']}

                *Lần kiểm tra cuối: {formatted_time}*
                """
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
                        
                summary_text = f"""### Trạng thái: {status_display}

                **Không phát hiện xung đột nào trong tài liệu này.**

                *Lần kiểm tra cuối: {formatted_time}*
                """
                summary_styles = {
                    'background': '#f0fdf4',
                    'padding': '10px',
                    'border-radius': '4px',
                    'border': '1px solid #bbf7d0',
                    'margin-bottom': '10px'
                }
            
            conflict_summary = pn.pane.Markdown(
                summary_text,
                styles=summary_styles
            )
            
            reload_button = pn.widgets.Button(
                name="Phân tích mâu thuẫn",
                button_type="primary",
                width=160
            )
            reload_button.on_click(lambda event: self.request_conflict_analysis(doc_id))
            
            model_status = pn.pane.Markdown(
                "**Model phân tích:** OpenAI (từ cache)",
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
            
            self.conflicts_container.append(
                pn.Row(
                    pn.pane.Markdown("### Thông tin mâu thuẫn (từ cache)", styles={
                        'color': '#2c5282',
                        'font-size': '18px',
                        'margin-bottom': '5px'
                    }),
                    pn.Spacer(width=20),
                    reload_button,
                    align='center',
                    sizing_mode='stretch_width'
                )
            )
            
            self.conflicts_container.append(conflict_summary)
            self.conflicts_container.append(pn.Row(model_status, align='end'))
            
            self.conflicts_container.append(
                pn.pane.Markdown(
                    "**Đang hiển thị dữ liệu từ bộ nhớ đệm để cải thiện hiệu suất.**\n\nNhấn nút 'Phân tích mâu thuẫn' để tải lại dữ liệu mới nhất.",
                    styles={
                        'color': '#2563eb',
                        'background': '#dbeafe',
                        'padding': '10px',
                        'border-radius': '4px',
                        'margin': '10px 0',
                        'font-size': '14px'
                    }
                )
            )
            
            content_conflicts_container = pn.Column(name="Mâu thuẫn nội dung")
            internal_conflicts_container = pn.Column(name="Mâu thuẫn nội bộ")
            external_conflicts_container = pn.Column(name="Mâu thuẫn ngoại bộ")
            
            if content_conflicts:
                content_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['content']} mâu thuẫn nội dung chunk**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '10px',
                            'border-radius': '4px',
                            'border-left': '4px solid #2563eb'
                        }
                    )
                )
                for i, conflict in enumerate(content_conflicts[:10]): 
                    conflict_card = self._create_conflict_card(conflict, "content")
                    content_conflicts_container.append(conflict_card)
                    
                if len(content_conflicts) > 10:
                    content_conflicts_container.append(
                        pn.pane.Markdown(
                            f"**...và {len(content_conflicts) - 10} mâu thuẫn khác. Nhấn 'Phân tích mâu thuẫn' để xem đầy đủ.**",
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
                
            if internal_conflicts:
                internal_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['internal']} mâu thuẫn nội bộ**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '10px',
                            'border-radius': '4px',
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
                
            if external_conflicts:
                external_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['external']} mâu thuẫn ngoại bộ**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '10px',
                            'border-radius': '4px',
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
            
        except Exception as e:
            logger.error(f"Error displaying cached conflicts: {str(e)}")
            logger.error(traceback.format_exc())
            
            self._is_loading_conflicts = False
            self.load_conflicts_data(doc_id)
    
    def load_conflicts_data(self, doc_id):
        """
        Load and display conflict information for a document with improved performance
        
        Args:
            doc_id (str): The ID of the document to load conflicts for
        """
        try:
            cache_key = f"{doc_id}_conflicts"
            current_time = time.time()
            cache_timestamp = getattr(self, '_conflict_cache_timestamps', {}).get(cache_key, 0)
            use_cache = False
            
            if hasattr(self, '_conflict_info_cache') and cache_key in self._conflict_info_cache:
                if current_time - cache_timestamp < 10: 
                    document = self.data_manager.get_document_by_id(doc_id)
                    if document:
                        conflict_status = document.get('conflict_status', 'No Conflict')
                        conflict_analysis_status = document.get('conflict_analysis_status', 'NotAnalyzed')
                        if conflict_status != 'Analyzing' and conflict_analysis_status != 'Analyzing':
                            use_cache = True
                            
            if hasattr(self, '_is_loading_conflicts') and self._is_loading_conflicts:
                if use_cache:
                    logger.info(f"Using cached conflicting information for {doc_id}")
                    conflict_info = self._conflict_info_cache[cache_key]
                    self._display_cached_conflicts(conflict_info, doc_id)
                    return
                else:
                    logger.info(f"Already loading conflict for {doc_id}, ignoring duplicate request")
                    return
                
            self._is_loading_conflicts = True
                    
            if hasattr(self, 'conflicts_container'):
                self.conflicts_container.clear()
            else:
                self.conflicts_container = pn.Column(sizing_mode='stretch_width')
                        
            self.clear_messages()
            
            if not hasattr(self, '_conflict_loading_ui'):
                loading_spinner = pn.indicators.LoadingSpinner(value=True, size=40)
                loading_message = pn.pane.Markdown("### Đang tải thông tin mâu thuẫn...", 
                                                styles={'color': '#4A5568', 'text-align': 'center', 'padding': '20px'})
                self._conflict_loading_ui = pn.Column(loading_spinner, loading_message, align='center')
            
            self.conflicts_container.append(self._conflict_loading_ui)
                            
            document = self.data_manager.get_document_by_id(doc_id)
            if not document:
                self.show_error_message("Không tìm thấy tài liệu")
                self.conflicts_container.clear()
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
                self._is_loading_conflicts = False
                return

            conflict_analysis_status = document.get('conflict_analysis_status', 'NotAnalyzed')
            conflict_status = document.get('conflict_status', 'No Conflict')
            has_conflicts = document.get('has_conflicts', False)
            duplicate_group_id = document.get('duplicate_group_id')
            
            logger.info(f"Document {doc_id} - conflict_status: {conflict_status}, " 
                        f"has_conflicts: {has_conflicts}, "
                        f"conflict_analysis_status: {conflict_analysis_status}, "
                        f"duplicate_group_id: {duplicate_group_id}")
            
            if duplicate_group_id and (conflict_status == 'Analyzing' or has_conflicts):
                if not hasattr(self, 'conflict_manager'):
                    self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                        
                sync_result = self.conflict_manager.sync_group_conflicts(doc_id)
                logger.info(f"Synced conflicts for document {doc_id} in group {duplicate_group_id}")
                
                document = self.data_manager.get_document_by_id(doc_id)
                if document:
                    conflict_status = document.get('conflict_status', conflict_status)
                    has_conflicts = document.get('has_conflicts', has_conflicts)
                    conflict_analysis_status = document.get('conflict_analysis_status', conflict_analysis_status)
                        
            analyzing_time = None
            long_analyzing = False
            analyzing_seconds = 0
                    
            if conflict_analysis_status == 'Analyzing' or conflict_status == 'Analyzing':
                last_status_update = document.get('modified_date')
                if last_status_update:
                    if isinstance(last_status_update, str):
                        try:
                            last_status_update = datetime.fromisoformat(last_status_update)
                        except:
                            last_status_update = None
                                
                    if last_status_update:
                        analyzing_seconds = (datetime.now() - last_status_update).total_seconds()
                        if analyzing_seconds > 180:  
                            long_analyzing = True
                            analyzing_time = int(analyzing_seconds / 60)
            
            if long_analyzing:
                self._stop_infinite_analyzing(doc_id)
                self._is_loading_conflicts = False
                return

            elif conflict_analysis_status == 'Analyzing' or conflict_status == 'Analyzing':
                self.conflicts_container.clear()
                self.conflicts_container.append(
                    pn.Column(
                        pn.indicators.LoadingSpinner(value=True, size=40),
                        pn.pane.Markdown("### Đang phân tích mâu thuẫn ...\nVui lòng đợi...", styles={
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
                
                if analyzing_seconds > 60: 
                    cancel_button = pn.widgets.Button(
                        name="Hủy phân tích",
                        button_type="danger",
                        width=120
                    )
                    cancel_button.on_click(lambda event: self._stop_infinite_analyzing(doc_id))
                    
                    self.conflicts_container.append(
                        pn.Row(
                            pn.pane.Markdown(f"Phân tích đang chạy trong {int(analyzing_seconds)}s", 
                                            styles={'color': '#718096', 'font-size': '14px'}),
                            cancel_button,
                            align='center'
                        )
                    )
                
                if (not hasattr(self, '_conflict_reload_count') or self._conflict_reload_count < 5):
                    if hasattr(self, '_conflict_reload_count'):
                        self._conflict_reload_count += 1
                    else:
                        self._conflict_reload_count = 1
                    
                    def delayed_reload():
                        if (hasattr(self, 'current_doc_id') and self.current_doc_id == doc_id and
                            hasattr(self, 'tabs') and self.tabs.active == 3):
                            logger.info(f"Auto-reloading conflicts (attempt {self._conflict_reload_count})")
                            if not hasattr(self, '_is_loading_conflicts') or not self._is_loading_conflicts:
                                self.load_conflicts_data(doc_id)
                    
                    pn.state.add_periodic_callback(delayed_reload, 5000, count=1)
                    logger.info(f"Schedule reload after 5 seconds for document {doc_id}")
                
                self._is_loading_conflicts = False
                return
                    
            conflict_info = document.get('conflict_info', '{}')
            if conflict_info is None:
                conflict_info = '{}'

            try:
                if isinstance(conflict_info, str):
                    conflict_info = json.loads(conflict_info)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in conflict_info: {conflict_info}")
                conflict_info = {
                    "content_conflicts": [],
                    "internal_conflicts": [],
                    "external_conflicts": []
                }

            if not isinstance(conflict_info, dict):
                conflict_info = {
                    "content_conflicts": [],
                    "internal_conflicts": [],
                    "external_conflicts": []
                }
            
            if not hasattr(self, '_conflict_info_cache'):
                self._conflict_info_cache = {}
            self._conflict_info_cache[cache_key] = conflict_info
            
            if not hasattr(self, '_conflict_cache_timestamps'):
                self._conflict_cache_timestamps = {}
            self._conflict_cache_timestamps[cache_key] = current_time
            
            content_conflicts = conflict_info.get('content_conflicts', [])
            internal_conflicts = conflict_info.get('internal_conflicts', [])
            external_conflicts = conflict_info.get('external_conflicts', [])
            
            logger.info(f"Document {doc_id} - content_conflicts: {len(content_conflicts)}, " 
                    f"internal_conflicts: {len(internal_conflicts)}, "
                    f"external_conflicts: {len(external_conflicts)}")
            
            if not hasattr(self, 'conflict_manager'):
                self.conflict_manager = ConflictManager(self.data_manager, self.chroma_manager)
                        
            conflict_counts = {
                'content': len(content_conflicts),
                'internal': len(internal_conflicts),
                'external': len(external_conflicts),
                'total': len(content_conflicts) + len(internal_conflicts) + len(external_conflicts)
            }
            
            has_conflicts = has_conflicts or conflict_counts['total'] > 0
            last_check = document.get('last_conflict_check')
            
            if has_conflicts != bool(conflict_counts['total'] > 0):
                has_conflicts = bool(conflict_counts['total'] > 0)
                conflict_status = 'Pending Review' if has_conflicts else 'No Conflict'
                
                try:
                    self.data_manager.update_document_status(doc_id, {
                        'has_conflicts': has_conflicts,
                        'conflict_status': conflict_status,
                        'needs_conflict_reanalysis': False 
                    })
                except Exception as update_error:
                    logger.error(f"Error updating conflict status: {str(update_error)}")
            
            if last_check:
                if isinstance(last_check, str):
                    try:
                        last_check = datetime.fromisoformat(last_check)
                        formatted_time = last_check.strftime('%d/%m/%Y %H:%M:%S')
                    except ValueError:
                        formatted_time = last_check
                else:
                    formatted_time = str(last_check)
            else:
                formatted_time = "Chưa kiểm tra"
            
            if has_conflicts:
                status_display = "Có mâu thuẫn"
                if conflict_status in ["Pending Review", "Resolving", "Conflict"]:
                    status_display = "Có mâu thuẫn cần xem xét"
                        
                summary_text = f"""### Trạng thái: {status_display}

                **Tổng số mâu thuẫn phát hiện: {conflict_counts['total']}**
                - Mâu thuẫn nội dung: {conflict_counts['content']}
                - Mâu thuẫn nội bộ: {conflict_counts['internal']}
                - Mâu thuẫn ngoại bộ: {conflict_counts['external']}

                *Lần kiểm tra cuối: {formatted_time}*
                """
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
                        
                summary_text = f"""### Trạng thái: {status_display}

                **Không phát hiện xung đột nào trong tài liệu này.**

                *Lần kiểm tra cuối: {formatted_time}*
                """
                summary_styles = {
                    'background': '#f0fdf4',
                    'padding': '10px',
                    'border-radius': '4px',
                    'border': '1px solid #bbf7d0',
                    'margin-bottom': '10px'
                }
            
            # Tạo các phần tử UI
            conflict_summary = pn.pane.Markdown(
                summary_text,
                styles=summary_styles
            )
            
            reload_button = pn.widgets.Button(
                name="Phân tích mâu thuẫn",
                button_type="primary",
                width=160
            )
            reload_button.on_click(lambda event: self.request_conflict_analysis(doc_id))
            
            self.conflicts_container.clear()
            
            model_status = pn.pane.Markdown(
                "**Model phân tích:** OpenAI",
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
            
            content_conflicts_container = pn.Column(name="Mâu thuẫn nội dung")
            internal_conflicts_container = pn.Column(name="Mâu thuẫn nội bộ")
            external_conflicts_container = pn.Column(name="Mâu thuẫn ngoại bộ")

            if content_conflicts:
                content_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['content']} mâu thuẫn nội dung chunk**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '10px',
                            'border-radius': '4px',
                            'border-left': '4px solid #2563eb'
                        }
                    )
                )
                for i, conflict in enumerate(content_conflicts[:20]): 
                    conflict_card = self._create_conflict_card(conflict, "content")
                    content_conflicts_container.append(conflict_card)
                    
                if len(content_conflicts) > 20:
                    content_conflicts_container.append(
                        pn.pane.Markdown(
                            f"**...và {len(content_conflicts) - 20} mâu thuẫn khác.**",
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

            if internal_conflicts:
                internal_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['internal']} mâu thuẫn nội bộ**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '10px',
                            'border-radius': '4px',
                            'border-left': '4px solid #2563eb'
                        }
                    )
                )
                for i, conflict in enumerate(internal_conflicts[:20]):
                    conflict_card = self._create_conflict_card(conflict, "internal")
                    internal_conflicts_container.append(conflict_card)
                    
                if len(internal_conflicts) > 20:
                    internal_conflicts_container.append(
                        pn.pane.Markdown(
                            f"**...và {len(internal_conflicts) - 20} mâu thuẫn khác.**",
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

            if external_conflicts:
                external_conflicts_container.append(
                    pn.pane.Markdown(
                        f"**Đã phát hiện {conflict_counts['external']} mâu thuẫn ngoại bộ**",
                        styles={
                            'color': '#2563eb',
                            'font-size': '16px',
                            'font-weight': 'bold',
                            'margin': '10px 0 20px 0',
                            'background': '#edf7ff',
                            'padding': '10px',
                            'border-radius': '4px',
                            'border-left': '4px solid #2563eb'
                        }
                    )
                )
                for i, conflict in enumerate(external_conflicts[:20]):
                    conflict_card = self._create_conflict_card(conflict, "external")
                    external_conflicts_container.append(conflict_card)
                    
                if len(external_conflicts) > 20:
                    external_conflicts_container.append(
                        pn.pane.Markdown(
                            f"**...và {len(external_conflicts) - 20} mâu thuẫn khác.**",
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
            
            self.conflicts_container.append(
                pn.Row(
                    pn.pane.Markdown("### Thông tin mâu thuẫn", styles={
                        'color': '#2c5282',
                        'font-size': '18px',
                        'margin-bottom': '5px'
                    }),
                    pn.Spacer(width=20),
                    reload_button,
                    align='center',
                    sizing_mode='stretch_width'
                )
            )
            
            self.conflicts_container.append(conflict_summary)
            self.conflicts_container.append(pn.Row(model_status, align='end'))
            self.conflicts_container.append(conflict_tabs)
            
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
                    width=160
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

            total_conflicts = conflict_counts['total']
            if total_conflicts > 0:
                summary = f"Tổng số mâu thuẫn: {total_conflicts} "
                summary += f"(Nội dung: {conflict_counts['content']}, Nội bộ: {conflict_counts['internal']}, Ngoại bộ: {conflict_counts['external']})"
                self.show_info_message(summary)
            else:
                self.show_info_message("Không phát hiện xung đột trong tài liệu này")
                
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
                    
            self.conflict_monitoring_callback = pn.state.add_periodic_callback(
                self.monitor_conflict_analysis,
                period
            )
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