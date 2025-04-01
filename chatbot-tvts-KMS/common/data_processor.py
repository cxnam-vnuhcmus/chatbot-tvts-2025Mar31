import os
import json
from datetime import datetime
import traceback
from common.data_manager import DatabaseManager
from dotenv import load_dotenv
load_dotenv()
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) 

class DataProcessor:
    def __init__(self):
        self.data_manager = DatabaseManager()
        current_dir = os.path.dirname(os.path.abspath(__file__))  
        kms_dir = os.path.abspath(os.path.join(current_dir)) 

        project_root = os.path.abspath(os.path.join(current_dir, '..'))
        permissions_file = os.path.join(project_root, 'conf', 'permissions.json')
        with open(permissions_file, 'r') as f:
            self.permissions = json.load(f)
            
    def get_user_unit(self, username):
        """
            Retrieves the unit associated with a given username.

            Args:
                username (str): The username for which the unit is to be retrieved.

            Returns:
                str: The unit associated with the username. Returns an empty string if no unit is found.
        """
        return self.permissions.get(username, {}).get('unit', '')

    def submit_for_review(self, doc_data, username):
        """
        Submits a document for review by associating it with a user's unit.

        Args:
            doc_data (dict): A dictionary containing the document data to be submitted.
            username (str): The username of the user submitting the document.

        Returns:
            str: The unique ID of the submitted document if successful.
            
        Raises:
            Exception: If there is an error during the submission process.
        """
        try:
            unit = self.get_user_unit(username)
            if not unit:
                raise ValueError(f"Không tìm thấy unit cho user {username}")
                
            doc_data['username'] = username
            
            doc_id = self.data_manager.submit_document(doc_data, unit)
            if not doc_id:
                raise Exception("Cannot save document")
                
            logger.info(f"Document {doc_id} submitted successfully by {username}")
            return doc_id
            
        except Exception as e:
            logger.error(f"Error in submit_for_review: {str(e)}")
            logger.error(traceback.format_exc())
            raise Exception(f"Error submitting document: {str(e)}")