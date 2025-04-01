import panel as pn
import param
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))) 
from common.data_manager import DatabaseManager
from common.data_processor import DataProcessor
from common.utils import remove_html
import requests
import logging
from concurrent.futures import ThreadPoolExecutor
import time 
import traceback 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) 

pn.extension(
    "texteditor", 
    "tabulator",
    "notifications",  
    sizing_mode="stretch_width",
    notifications=True
)

class KMSUser(param.Parameterized):
    def __init__(self, username):
        super().__init__()
        self._loading = False
        self._initial_loaded = False
        self._last_update_time = time.time()
        self._update_interval = 5000  
        self._auto_update_enabled = True 
        
        self.username = username
        self.data_manager = DatabaseManager()
        self.data_processor = DataProcessor()
        
        self.scanner_api = os.getenv('KMS_SCANNER_API')
        
        self.executor = ThreadPoolExecutor(max_workers=5)
        
        self._setup_ui()
        self._setup_initial_load()
    
    def _setup_ui(self):
        """
        Initializes and sets up the user interface (UI) components.

        Args:
            None: This method does not take any arguments.

        Returns:
            None: This method does not return any values. It initializes and sets up UI elements for display.
        """
        self.document_cards = pn.Column(
            pn.pane.Markdown("### Đang tải dữ liệu...", styles={'text-align': 'center'}),
            sizing_mode='stretch_width',
            styles={
                'padding': '20px',
                'background': '#ffffff',
                'min-height': '200px'
            }
        )
        self.create_widgets()
        self.create_layout()

    def _setup_initial_load(self):
        """
        Sets up the initial load callback that triggers the `initial_load` method 
        when the application or component is first loaded.

        Args:
            None: This method does not take any arguments.

        Returns:
            None: This method does not return any values. It performs a setup action 
                for the initial load callback.
        """
        if not hasattr(self, '_load_callback'):
            self._load_callback = pn.state.onload(self.initial_load)

    def initial_load(self, event=None):
        """
        Loads the initial set of documents with strict control to prevent redundant or 
        overlapping loading operations.

        If loading fails at any point, an error message is displayed to the user to notify them of the issue.

        Args:
            event (optional): An event parameter that may be used for handling specific triggers (default is None).

        Returns:
            None: This method does not return any values. It performs actions like loading data, refreshing UI, 
                and setting up automatic updates.
        """
        if self._loading or self._initial_loaded:
            return
            
        try:
            self._loading = True
            self._initial_loaded = True
            
            documents = self.data_manager.get_all_documents()
            if documents is not None:
                self.refresh_document_cards(documents)
                self._last_documents = documents.copy()
                self._last_update_time = time.time()
                
                pn.state.add_periodic_callback(
                    self._delayed_setup_auto_update,
                    1000,  
                    count=1  
                )
            else:
                self.show_error_message("Không thể tải dữ liệu ban đầu")
                
        except Exception as e:
            logger.error(f"Error loading initial data: {str(e)}")
            self.show_error_message("Không thể tải dữ liệu. Vui lòng thử lại sau.")
            
        finally:
            self._loading = False

    def _delayed_setup_auto_update(self):
        """
        Sets up the auto-update feature after a delay, enabling periodic updates.

        This method ensures that:
        - Auto-update is only set up if it is enabled (`_auto_update_enabled` is `True`).
        - If an existing update callback is present, it stops the previous callback before creating a new one.
        - The new update callback is added to periodically check for updates, using the `check_and_update` method at the specified interval (`_update_interval`).

        If an error occurs while setting up the auto-update, it logs an error message.

        Args:
            None: This method does not take any arguments.

        Returns:
            None: This method does not return any values. It performs an action to set up the auto-update callback.
        """
        if not self._auto_update_enabled:
            return
            
        try:
            if hasattr(self, 'update_callback') and self.update_callback is not None:
                self.update_callback.stop()
                
            self.update_callback = pn.state.add_periodic_callback(
                self.check_and_update,
                self._update_interval,
                start=True
            )
            logger.info(f"Auto-update set with period={self._update_interval}ms")
            
        except Exception as e:
            logger.error(f"Error setting up auto-update:: {str(e)}")

    def initial_load(self, event=None):
        """
        Safely initializes and loads the initial data set, ensuring that redundant or 
        overlapping loading operations are avoided.

        Args:
            event (optional): An event parameter that may be used for handling specific triggers 
                            (default is `None`).

        Returns:
            None: This method does not return any values. It performs actions like loading data, 
                refreshing UI, and setting up automatic updates.
        """
        if self._loading or self._initial_loaded:
            return
            
        try:
            self._loading = True
            self._initial_loaded = True
            
            documents = self.data_manager.get_all_documents()
            if documents is not None:
                self._update_document_cache(documents)
                
                self.refresh_document_cards(documents)
                self._last_update_time = time.time()
                
                pn.state.add_periodic_callback(
                    lambda: self.setup_auto_update(self._update_interval),
                    500,  # 500ms delay
                    count=1
                )
            else:
                self.show_error_message("Không thể tải dữ liệu ban đầu")
                
        except Exception as e:
            logger.error(f"Error loading initial data: {str(e)}")
            self.show_error_message("Không thể tải dữ liệu. Vui lòng thử lại sau.")
            
        finally:
            self._loading = False
    
    def _update_document_cache(self, documents):
        if documents is None:
            return
            
        new_cache = {
            'data_hash': hash(str(documents.values.tobytes())),
            'count': len(documents),
            'last_modified': documents['modified_date'].max() if 'modified_date' in documents.columns else None,
            'documents': documents
        }
        
        self._document_cache = new_cache
        self._last_documents = documents.copy()
                

    def refresh_document_cards(self, documents):
        """
        Refreshes the content of document cards and provides a quantity change notification.

        Args:
            documents (DataFrame): A Pandas DataFrame containing the documents to be displayed. The DataFrame must 
                                    include relevant columns like 'created_date' for sorting and displaying document info.

        Returns:
            None: This method does not return any values. It performs UI updates by refreshing the document cards 
                and displaying appropriate messages.
        """
        try:
            previous_count = len(self._last_documents) if hasattr(self, '_last_documents') else 0
            current_count = len(documents)

            temp_container = pn.Column(
                sizing_mode='stretch_width', 
                styles={
                    'padding': '20px',
                    'background': '#ffffff'
                }
            )

            header_text = f"### Tổng số tài liệu: {current_count} bản ghi"
            
            header = pn.pane.Markdown(
                header_text,
                styles={
                    'background': '#f0f7ff',
                    'padding': '15px',
                    'border-radius': '8px',
                    'margin-bottom': '20px',
                    'border': '1px solid #e3ebf6'
                }
            )
            temp_container.append(header)


            ######
            data_filters = {
                'id': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo ID'},
                'content': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo nội dung'},
                'created_date': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo ngày'},
                'unit': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo đơn vị'},
                'sender': {'type': 'input', 'func': 'like', 'placeholder': 'Tìm theo người tạo'},
                'approval_status': {'type': 'input', 'func': 'like', 'placeholder': ''},
                'is_duplicate': {'type': 'input', 'func': 'like', 'placeholder': ''},
                'conflict_status': {'type': 'input', 'func': 'like', 'placeholder': ''},
            }

            displayed_columns = [
                'id', 'content', 'created_date', 'unit', 'sender', 
                'approval_status', 
                'is_duplicate', 
                'conflict_status'
            ]

            column_titles = {
                'id': 'Mã tài liệu',
                'content': 'Nội dung',
                'created_date': 'Ngày tạo',
                'unit': 'Đơn vị',
                'sender': 'Người tạo',
                'approval_status': 'Trạng thái', 
                'is_duplicate': 'Có Trùng lắp',
                'conflict_status': 'Có Mâu thuẫn',
            }

            if documents is None or len(documents.columns) == 0:
                documents = pd.DataFrame(columns=displayed_columns)

            available_columns = [col for col in displayed_columns if col in documents.columns]
            if len(available_columns) == 0:
                available_columns = displayed_columns
            
            documents = documents.sort_values(by='created_date', ascending=False)
            documents = self._format_initial_data(documents)

            filtered_data = documents[available_columns] if len(documents) > 0 else pd.DataFrame(columns=available_columns)

            column_widths = {
                'id': '10%',               
                'content': '30%',
                'created_date': '10%',   
                'unit': '10%',           
                'approval_status': '10%', 
                'sender': '10%',
                'is_duplicate': '10%',
                'conflict_status': '10%',
            }

            center_aligned_columns = {'approval_status', 'is_duplicate', 'conflict_status'}  # Cột cần căn giữa

            column_configs = [
                {
                    'field': col,
                    'title': column_titles.get(col, col),
                    'width': column_widths.get(col),
                    'text_align': 'center' if col in center_aligned_columns else 'left',
                    'formatter': 'truncate' if col == 'content' else None,
                }
                for col in available_columns if col in column_widths
            ]

            data_table = pn.widgets.Tabulator(
                value=filtered_data,
                pagination='local',
                page_size=10, 
                selectable=False,
                header_filters=data_filters,
                min_width=1200,
                disabled=False,
                sizing_mode="stretch_width",
                styles={'height': '80vh'},
                show_index=False,
                theme='bootstrap5',
                theme_classes=['table-striped', 'table-bordered'],
                selection=[],
                configuration={
                    'layout': 'fitColumns',  
                    'columns': column_configs
                }
            )

            # if documents.empty:
            #     empty_message = pn.pane.Markdown(
            #         "Không có dữ liệu",
            #         styles={
            #             'text-align': 'center',
            #             'padding': '40px',
            #             'color': '#666',
            #             'font-style': 'italic',
            #             'background': '#f8f9fa',
            #             'border-radius': '8px'
            #         }
            #     )
            #     temp_container.append(empty_message)
            # else:
            #     documents = documents.sort_values(by='created_date', ascending=False)
            #     cards_container = pn.Column(
            #         sizing_mode='stretch_width',
            #         styles={'margin-top': '20px'}
            #     )
                
            #     for _, doc in documents.iterrows():
            #         try:
            #             card = self._create_document_card(doc)
            #             cards_container.append(card)
            #         except Exception as e:
            #             logger.error(f"Error creating card for document {doc.get('id')}: {str(e)}")
            #             continue

            #     temp_container.append(cards_container)

            self.document_cards.clear()
            self.document_cards.append(data_table)
            self.document_cards.visible = True

        except Exception as e:
            error_message = pn.pane.Markdown(
                f"❌ **Lỗi khi tải dữ liệu:** {str(e)}",
                styles={
                    'color': '#dc3545',
                    'padding': '15px',
                    'background': '#fff5f5',
                    'border-radius': '8px',
                    'margin-top': '10px'
                }
            )
            self.document_cards.clear()
            self.document_cards.append(error_message)

    def _format_initial_data(self, documents):
        try:
            if documents is None or len(documents) == 0:
                return
                
            if 'is_duplicate' in documents.columns:
                documents['is_duplicate'] = documents['is_duplicate'].apply(
                    lambda x: "Có trùng lắp" if x else "Không trùng lắp"
                )

            if 'conflict_status' in documents.columns:
                documents['conflict_status'] = documents['conflict_status'].apply(
                    lambda x: "Không mẫu thuẫn" if x == "No Conflict" else "Có mâu thuẫn"
                )
            return documents

        except Exception as e:
            logger.error(traceback.format_exc())       
          
    def show_error_message(self, message):
        """
        Displays an error message in the document cards section.

        Args:
            message (str): The error message to be displayed. This message will be formatted 
                        and shown to the user in the document cards section.

        Returns:
            None: This method does not return any values. It performs UI updates by displaying 
                the error message in the document cards area.
        """
        error_message = pn.pane.Markdown(
            f"❌ **{message}**",
            styles={
                'color': '#dc3545',
                'padding': '15px',
                'background': '#fff5f5',
                'border-radius': '8px',
                'margin-top': '10px',
                'text-align': 'center'
            }
        )
        self.document_cards.clear()
        self.document_cards.append(error_message)
        
    def create_widgets(self):
        self.header = pn.pane.Markdown(
            f"# HỆ THỐNG QUẢN LÝ TRI THỨC ({self.username} [logout](/logout))", 
            css_classes=['header'],
            styles={
                'background': '#f8f9fa',
                'padding': '20px',
                'border-bottom': '1px solid #dee2e6',
                'margin-bottom': '20px'
            }
        )

        self.content_input = pn.widgets.TextEditor(
            name="Nội dung", 
            placeholder="Nhập nội dung...",
            height=300, 
            sizing_mode='stretch_width'
        )

        multi_choice_categories = pn.widgets.MultiChoice(
            name="categories", 
            value=[], 
            options=['thông tin chung', 'chương trình đào tạo', 'hỗ trợ người học', 
                    'Đánh giá năng lực của ĐHQG-HCM', 'Đề án tuyển sinh', 'Công tác sinh viên'],
            sizing_mode="stretch_width",
        )
        self.categories_select = pn.Column(multi_choice_categories)

        multi_choice_tags = pn.widgets.MultiChoice(
            name="tags",
            value=[], 
            options=[
                'ĐH An Giang', 'ĐH Bách Khoa', 'ĐH CNTT', 'ĐH KT-Luật',
                'ĐH Quốc Tế', 'ĐH KHTN', 'ĐH KHXH-NV', 'Phân hiệu ĐHQG tại Tỉnh Bến Tre',
                'Học bổng sinh viên', 'Khảo sát việc làm sinh viên', 'KTX, Nội trú sinh viên',
                'Các hoạt động học thuật', 'Hoạt động khác', 'FAQs (Câu hỏi thường gặp)',
                'Đề thi mẫu', 'Danh sách trường dùng kết quả ĐGNL của ĐHQG-HCM để xét tuyển'
            ],
            sizing_mode="stretch_width",
        )
        self.tags_select = pn.Column(multi_choice_tags)

        self.tag_input = pn.widgets.TextInput(
            name="Tag mới",
            placeholder="Nhập tag mới và ngăn cách bằng dấu phẩy...",
            sizing_mode="stretch_width",
        )

        self.start_date = pn.widgets.DatetimePicker(
            name="Ngày bắt đầu hiệu lực",
            value=datetime.now(),
            width=250,
        )
        
        self.end_date = pn.widgets.DatetimePicker(
            name="Ngày kết thúc hiệu lực",
            width=250,
        )

        self.validity = pn.widgets.Checkbox(
            name="Cho phép tìm kiếm sau ngày hết hiệu lực",
            value=True,
            styles={'margin-top': '10px'}
        )

        button_width = 150
        button_height = 45

        self.add_button = pn.widgets.Button(
            name="Gửi để duyệt",
            button_type="primary",
            button_style="solid",
            width=button_width,
            height=button_height,
            styles={'font-size': '16px'}
        )
        self.add_button.on_click(self.submit_document)

        self.clear_button = pn.widgets.Button(
            name="Làm mới",
            button_type="default", 
            button_style="solid",
            width=button_width,
            height=button_height,
            styles={'font-size': '16px'}
        )
        self.clear_button.on_click(self.clear_input_fields)

        self.document_cards = pn.Column(
            sizing_mode='stretch_width',
            styles={'padding': '10px'}
        )
    
                
        self.result_view = pn.pane.Markdown(
            "",
            styles={
                'color': 'green',
                'padding': '10px',
                'margin': '10px 0',
                'border-radius': '4px'
            }
        )
    
    def create_layout(self):
        input_form = pn.Column(
            pn.pane.Markdown("### Nhập nội dung mới:", 
                styles={'margin-bottom': '5px', 'margin-top': '0px'}),
            self.content_input,
            pn.Spacer(height=5),

            pn.Row(
                pn.Column(
                    pn.pane.Markdown("### Lựa chọn categories:", 
                        styles={'margin-bottom': '5px', 'margin-top': '0px'}),
                    pn.Row(
                        self.categories_select[0],
                        sizing_mode='stretch_width'
                    ),
                    sizing_mode='stretch_width'
                ),
                sizing_mode='stretch_width',
                styles={'margin-bottom': '5px', 'margin-top': '0px'}
            ),

            pn.Row(
                pn.Column(
                    pn.pane.Markdown("### Lựa chọn tags:", 
                        styles={'margin-bottom': '5px', 'margin-top': '0px'}),
                    pn.Row(
                        self.tags_select[0],  
                        sizing_mode='stretch_width'
                    ),
                    pn.Row(
                        self.tag_input, 
                        sizing_mode='stretch_width'
                    ),
                    sizing_mode='stretch_width'
                ),
                sizing_mode='stretch_width',
                styles={'margin-bottom': '5px', 'margin-top': '0px'}
            ),
                
            pn.Spacer(height=5),

            pn.Row(
                pn.Column(
                    pn.pane.Markdown("### Thời gian hiệu lực:", 
                        styles={'margin-bottom': '5px', 'margin-top': '0px'}),
                    self.start_date,
                    self.end_date,
                    self.validity,
                ),
                sizing_mode='stretch_width'
            ),
            
            pn.Spacer(height=10),

            pn.Row(
                pn.Column(
                    pn.Row(
                        pn.Spacer(width=20), 
                        self.add_button,
                        pn.Spacer(width=20),
                        self.clear_button,
                        pn.Spacer(width=20),  
                        align='center',
                        sizing_mode='stretch_width'
                    ),
                    sizing_mode='stretch_width'
                ),
                sizing_mode='stretch_width',
                align='center'
            ),
            
            pn.Spacer(height=5),
            self.result_view,
            
            styles={
                'background': '#ffffff',
                'padding': '20px',
                'border-radius': '8px', 
                'box-shadow': '0 1px 3px rgba(0,0,0,0.1)',
                'margin': '0 auto'
            },
            sizing_mode='stretch_width'
        )

        menu_items = [
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

        main_tabs = pn.Tabs(
            ('Thêm nội dung', input_form),
            ('Danh sách', self.document_cards),
            dynamic=True,
            styles={
                'margin-top': '10px',
                'margin-bottom': '10px',
                'padding': '0 15px'
            }
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
            title="KMS - HỆ THỐNG QUẢN LÝ TRI THỨC",
            favicon="assets/images/favicon.png",
            main=[main_tabs],
            theme_toggle=False
        )
    
    def get_default_data(self):
        """
        Creates an empty DataFrame with predefined columns but no rows.

        Args:
            None: This method does not take any arguments.

        Returns:
            pd.DataFrame: An empty Pandas DataFrame with the following columns:
                        ['id', 'content', 'categories', 'tags', 'start_date', 'end_date', 
                        'unit', 'sender', 'created_date', 'is_valid', 'approval_status', 
                        'is_duplicate'].
        """
        return pd.DataFrame(columns=[
            'id', 'content', 'categories', 'tags', 
            'start_date', 'end_date', 'unit', 'sender',
            'created_date', 'is_valid', 'approval_status', 'is_duplicate'
        ])

    def clear_input_fields(self, event=None, clear_message=True):
        self.content_input.value = ""
        self.categories_select[0].value = []
        self.tags_select[0].value = []
        self.tag_input.value = ""
        self.start_date.value = datetime.now()
        self.end_date.value = None
        self.validity.value = True
        if clear_message:
            self.result_view.object = ""
            self.result_view.visible = False
    
    def submit_document(self, event=None):
        try:          
            self.add_button.disabled = True
            self.add_button.name = "Đang xử lý..."

            raw_content = self.content_input.value.strip()
            if not raw_content:
                self.show_message("❌ Lỗi: Nội dung không được để trống!", "red", 2000)
                return

            start_date = self.start_date.value
            end_date = self.end_date.value
            if end_date and start_date and end_date < start_date:
                self.show_message("❌ Lỗi: Ngày kết thúc phải sau ngày bắt đầu!", "red", 2000)
                return

            categories = list(self.categories_select[0].value)
            tags = list(self.tags_select[0].value)
            
            if self.tag_input.value:
                new_tags = [tag.strip() for tag in self.tag_input.value.split(',') if tag.strip()]
                tags.extend(new_tags)

            doc_data = {
                'content': remove_html(raw_content),
                'categories': categories,
                'tags': tags,
                'start_date': start_date,
                'end_date': end_date,
                'username': self.username,
                'is_valid': self.validity.value,
                'processing_status': 'Pending',
                'scan_status': 'Pending',
                'chunk_status': 'Pending',
                'approval_status': 'Pending'
            }

            doc_id = self.data_processor.submit_for_review(doc_data, self.username)
            if not doc_id:
                raise Exception("Không thể lưu tài liệu vào database")

            scanner_success = self.send_to_scanner(doc_id)

            self.clear_input_fields(clear_message=False)
            time.sleep(1)  
            self.update_all()

            if scanner_success:
                success_msg = f"""✅ Tài liệu đã được gửi thành công:
                - ID: {doc_id}
                - Trạng thái: Đang chờ duyệt
                - Scanner: Đã gửi và đang xử lý
                """
                self.show_message(success_msg, "green", 2000)
            else:
                warning_msg = f"""⚠️ Tài liệu đã được lưu nhưng có vấn đề:
                - ID: {doc_id}
                - Trạng thái: Đã lưu
                - Scanner: Chưa gửi được, sẽ thử lại sau
                """
                self.show_message(warning_msg, "orange", 2000)

        except Exception as e:
            error_msg = f"""❌ Lỗi khi gửi tài liệu:
            - Chi tiết: {str(e)}
            - Vui lòng thử lại hoặc liên hệ admin
            """
            self.show_message(error_msg, "red", 2000)

        finally:
            self.add_button.disabled = False
            self.add_button.name = "Gửi để duyệt"

    def send_to_scanner(self, doc_id):
        """
        Sends a document ID to the scanner service for processing with improved error handling.
        
        Args:
            doc_id (str): The document ID to send to the scanner.

        Returns:
            bool: True if the document was successfully sent, False if there was an error.
        """
        try:
            logger.info(f"Send document {doc_id} to scanner API: {self.scanner_api}")
            
            if not self.scanner_api:
                return False
                
            response = requests.post(
                f"{self.scanner_api}/scan_doc",
                json={"doc_id": doc_id},
                timeout=10, 
                headers={'Content-Type': 'application/json'}
            )
            
            logger.info(f"Scanner API response: Status={response.status_code}, Content={response.text[:100]}...")
            
            response.raise_for_status()
            return True
                    
        except requests.ConnectionError as e:
            time.sleep(2)
            try:
                response = requests.post(
                    f"{self.scanner_api}/scan_doc",
                    json={"doc_id": doc_id},
                    timeout=10,
                    headers={'Content-Type': 'application/json'}
                )
                response.raise_for_status()
                return True
            except Exception as retry_error:
                return False
        except Exception as e:
            logger.error(traceback.format_exc())
            return False
    
    def setup_auto_update(self, period=5000):
        """Set up automatic updates with shorter check intervals"""
        try:
            if hasattr(self, 'update_callback') and self.update_callback is not None:
                self.update_callback.stop()
                self.update_callback = None
                
            self._update_interval = max(period, 3000)
            self.update_callback = pn.state.add_periodic_callback(
                self.check_and_update,
                self._update_interval,
                start=True
            )
            logger.info(f"Auto-update set with period={self._update_interval}ms")
            
        except Exception as e:
            logger.error(f"Error setting up auto-update: {str(e)}")

    def check_and_update(self):
        current_time = time.time()
        
        if current_time - self._last_update_time < (self._update_interval / 1000) or self._loading:
            return
            
        try:
            self._loading = True
            documents = self.data_manager.get_all_documents()
            
            if documents is not None and hasattr(self, '_last_documents'):
                current_ids = set(documents['id'])
                previous_ids = set(self._last_documents['id']) 
                
                deleted_ids = previous_ids - current_ids
                if deleted_ids:
                    self.show_deleted_notification(deleted_ids)
                    self.refresh_document_cards(documents)
                    
                else:
                    for _, new_doc in documents.iterrows():
                        old_doc = self._last_documents[self._last_documents['id'] == new_doc['id']]
                        if not old_doc.empty:
                            if (old_doc.iloc[0]['approval_status'] != new_doc['approval_status'] or
                                old_doc.iloc[0]['is_duplicate'] != new_doc['is_duplicate']):
                                self.refresh_document_cards(documents)
                                break
                                
                self._last_documents = documents.copy()
                
            self._last_update_time = current_time
                
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật: {str(e)}")
            
        finally:
            self._loading = False
    
    def show_deleted_notification(self, deleted_ids):
        try:
            if len(deleted_ids) == 1:
                pn.state.notifications.warning(
                    f"🗑️ Tài liệu {list(deleted_ids)[0]} đã bị xóa", 
                    duration=2000
                )
            else:
                pn.state.notifications.warning(
                    f"🗑️ {len(deleted_ids)} tài liệu đã bị xóa",
                    duration=2000 
                )
        except Exception as e:
            logger.error(f"Lỗi hiển thị thông báo xóa: {str(e)}")
        
    def update_all(self):
        if self._loading:
            return
            
        try:
            self._loading = True
            documents = self.data_manager.get_all_documents()
            if documents is not None:
                self.refresh_document_cards(documents)
                self._last_documents = documents.copy()
                self._last_update_time = time.time()
                
        except Exception as e:
            logger.error(f"Error while forcing update: {str(e)}")
            
        finally:
            self._loading = False
    
    def _create_document_card(self, doc):
        """
        Create a card that displays document information with view more/collapse feature
        """
        status_map = {
            'Pending': ('Chờ duyệt', '#FFA500'),
            'Approved': ('Đã duyệt', '#28a745'),
            'Rejected': ('Từ chối', '#dc3545')
        }
        
        status = doc.get('approval_status', 'Pending')
        if status not in status_map:
            status = 'Pending'
            
        status_text, status_color = status_map[status]

        created_date = (doc['created_date'].strftime('%Y-%m-%d %H:%M:%S') 
                    if pd.notnull(doc['created_date']) else "N/A")
        approver = doc.get('approver', '') or "Chưa có"
        approval_date = doc.get('approval_date')
        approval_date_text = (approval_date.strftime('%Y-%m-%d %H:%M:%S') 
                            if pd.notnull(approval_date) else "Chưa phê duyệt")

        is_duplicate = doc.get('is_duplicate', False)
        similarity_score = doc.get('similarity_score', 0)
        duplicate_group_id = doc.get('duplicate_group_id')
        original_doc_id = doc.get('original_chunked_doc')
        
        duplicate_info = ""
        if is_duplicate and duplicate_group_id:
            if original_doc_id:
                duplicate_info = f"""
                🔄 **Thông tin trùng lắp:**
                - Thuộc nhóm: {duplicate_group_id}
                - Tài liệu gốc: {original_doc_id}
                - Độ tương đồng: {similarity_score * 100:.1f}%
                """
            else:
                duplicate_info = f"""
                🔄 **Thông tin trùng lắp:**
                - Thuộc nhóm: {duplicate_group_id}
                - Độ tương đồng: {similarity_score * 100:.1f}%
                """

        expand_var = pn.widgets.Toggle(
            name='', 
            value=False,
            width=100,
            height=30,
            margin=(0, 0),
            stylesheets=['.bk-btn { font-size: 12px; border-radius: 15px; background-color: #f0f0f0; border: none; }']
        )

        full_content = doc.get('content', '')
        max_length = int(os.environ.get("MAX_LENGTH"))
        short_content = full_content[:max_length] + "..." if len(full_content) > max_length else full_content

        content_pane = pn.pane.Markdown(
            short_content,
            styles={
                'padding': '10px',
                'background': '#f8f9fa',
                'border-radius': '4px',
                'border': '1px solid #e9ecef',
                'font-size': '14px',
                'line-height': '1.5',
                'margin-top': '5px'
            }
        )

        def update_content(event):
            if event.new:
                content_pane.object = full_content
                expand_var.name = '📖 Thu gọn'  
            else:
                content_pane.object = short_content
                expand_var.name = '📖 Xem thêm'  

        expand_var.param.watch(update_content, 'value')
        expand_var.name = '📖 Xem thêm' 

        card = pn.Card(
            pn.Column(
                pn.Row(
                    pn.pane.Markdown(
                        f"### ID: {doc.get('id', 'N/A')}", 
                        styles={'margin': '0', 'font-size': '15px'}
                    ),
                    pn.pane.Markdown(
                        status_text,
                        styles={
                            'background': status_color,
                            'color': 'white',
                            'padding': '3px 10px',
                            'border-radius': '12px',
                            'margin-left': 'auto',
                            'font-weight': 'bold',
                            'font-size': '13px'
                        }
                    ),
                    sizing_mode='stretch_width',
                    margin=(0, 0, 5, 0)
                ),
                
                pn.layout.Divider(margin=(5, 0)),
                
                pn.Row(
                    pn.Column(
                        pn.pane.Markdown(
                            f"👤 **Người gửi:** {doc.get('sender', 'N/A')}",
                            styles={'font-size': '13px', 'margin': '0 0 3px 0'}
                        ),
                        pn.pane.Markdown(
                            f"🕒 **Ngày tạo:** {created_date}",
                            styles={'font-size': '13px', 'margin': '0 0 3px 0'}
                        ),
                        pn.pane.Markdown(
                            f"🏢 **Đơn vị:** {doc.get('unit', 'N/A')}",
                            styles={'font-size': '13px', 'margin': '0'}
                        ),
                        margin=(0, 10, 0, 0)
                    ),
                    pn.Column(
                        pn.pane.Markdown(
                            f"✍️ **Người duyệt:** {approver}",
                            styles={'font-size': '13px', 'margin': '0 0 3px 0'}
                        ),
                        pn.pane.Markdown(
                            f"📅 **Ngày duyệt:** {approval_date_text}",
                            styles={'font-size': '13px', 'margin': '0'}
                        ),
                        margin=(0, 0, 0, 10)
                    ),
                    sizing_mode='stretch_width'
                ),

                pn.pane.Markdown(
                    duplicate_info,
                    styles={
                        'background': '#fff3e0' if is_duplicate else 'transparent',
                        'padding': '8px',
                        'border-radius': '4px',
                        'margin-top': '5px',
                        'font-size': '13px'
                    }
                ) if duplicate_info else None,
                
                pn.Row(
                    pn.pane.Markdown(
                        "📝 **Nội dung:**",
                        styles={'font-size': '13px', 'margin': '0', 'flex': '1'}
                    ),
                    pn.pane.HTML(
                        "" if len(full_content) <= 500 else None,
                        styles={'flex': '1'}
                    ),
                    expand_var if len(full_content) > 500 else None,
                    styles={'align-items': 'center', 'margin-bottom': '5px'},
                    sizing_mode='stretch_width'
                ),
                content_pane,
                
                sizing_mode='stretch_width',
                styles={'padding': '10px'}
            ),
            styles={
                'margin-bottom': '15px',
                'border-left': f'4px solid {status_color}',
                'border-radius': '6px',
                'box-shadow': '0 1px 3px rgba(0,0,0,0.1)',
                'background': '#ffffff'
            }
        )
        return card
    
    def show_message(self, message, color="green", duration=2000):
        """
        Displays a notification message with automatic hiding after a specified duration.

        This method:
        - Displays a message to the user with the specified color and duration.
        - The message will automatically disappear after the given duration (in milliseconds).
        - The default color is green, but other options such as red and orange are also supported.
        - If an error occurs while displaying or hiding the message, a fallback error message is shown.

        Args:
            message (str): The content of the message to be displayed.
            color (str): The color of the message. Options are 'green', 'red', and 'orange' (default is 'green').
            duration (int): The duration for which the message is displayed (in milliseconds). 
                            The default duration is 2000ms (2 seconds).

        Returns:
            None: This method does not return any values. It performs UI updates by showing and hiding the message.
        """
        try:
            if hasattr(self, '_hide_message_callback') and self._hide_message_callback:
                self._hide_message_callback.stop()
                self._hide_message_callback = None

            self.result_view.object = message
            self.result_view.styles = {
                'color': color,
                'padding': '10px',
                'margin': '10px 0',
                'border-radius': '4px',
                'background': f'rgba({",".join(["0,255,0,.1" if color == "green" else "255,0,0,.1"])})',
                'transition': 'opacity 0.5s ease-in-out'
            }
            self.result_view.visible = True

            if duration > 0:
                def _hide_message():
                    try:
                        self.result_view.object = ""
                        self.result_view.visible = False
                    except Exception as e:
                        logger.error(f"Lỗi khi ẩn message: {str(e)}")

                # Use periodic callback with count=1 to run once
                self._hide_message_callback = pn.state.add_periodic_callback(
                    _hide_message,
                    duration,
                    count=1
                )

        except Exception as e:
            logger.error(f"Error displaying message: {str(e)}")
            self.result_view.object = "❌ Có lỗi xảy ra"
            self.result_view.visible = True
            
    def get_layout(self):
        return self.layout