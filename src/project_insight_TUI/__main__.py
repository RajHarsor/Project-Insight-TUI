from textual.app import App
from .elements.menu_screen import MenuScreen  # Import the MenuScreen class from menu_screen.py
from .elements.add_user import AddUserScreen  # Import the AddUserScreen class from add_user.py
from .elements.success_screen import SuccessScreen  # Import the SuccessScreen class from success_screen.py
from .elements.view_user_screen import ViewUserScreen  # Import the ViewUserScreen class from view_user_screen.py
from .elements.edit_user_screen import EditUserScreen  # Import the EditUserScreen class from edit_user_screen.py
from .elements.delete_user_screen import DeleteUserScreen  # Import the DeleteUserScreen class from delete_user_screen.py
from .elements.initialize_screen import InitializeCredentialsScreen  # Import the InitializeCredentialsScreen class from initialize_credentials.py
from .elements.send_sms_screen import SendSMSScreen  # Import the SendSMSScreen class from send_sms_screen.py
from .elements.confirm_add_user import ConfirmAddUserScreen  # Import the ConfirmAddUserScreen class from confirm_add_user.py
from .elements.initialize_incomplete_credentials_screen import InitializeIncompleteCredentialsScreen  # Import the InitializeIncompleteCredentialsScreen class from initialize_incomplete_credentials_screen.py
from .elements.initialize_no_env_file_screen import InitializeNoEnvFileScreen  # Import the InitializeNoEnvFileScreen class from initialize_no_env_file_screen.py
from .elements.send_sms_confirmation_screen import SendSMSConfirmationScreen  # Import the SendSMSConfirmationScreen class from send_sms_confirmation_screen.py
from .elements.update_env_file_screen import UpdateEnvFileScreen  # Import the UpdateEnvFileScreen class from update_env_file_screen.py

class MainGUI(App):
    TITLE = "Project Insight GUI"
    def on_mount(self) -> None:
        self.theme = 'nord'
        self.install_screen(MenuScreen(), name = "menu")
        
        self.install_screen(AddUserScreen(), name = "add_user")
        self.install_screen(ConfirmAddUserScreen(participant_id = None, study_start_date = None, study_end_date = None, phone_number = None, schedule_type = None, lb_link = None), name = "confirm_add_user")
        
        self.install_screen(SuccessScreen(), name = "success")
        self.install_screen(ViewUserScreen(), name = "view_user")
        self.install_screen(EditUserScreen(), name = "edit_user")
        self.install_screen(DeleteUserScreen(), name = "delete_user")
        
        self.install_screen(InitializeCredentialsScreen(), name = "initialize_credentials")
        self.install_screen(InitializeIncompleteCredentialsScreen(), name = "initialize_incomplete_credentials")
        self.install_screen(InitializeNoEnvFileScreen(), name = "initialize_no_env_file")
        self.install_screen(UpdateEnvFileScreen(), name = "update_env_file")
        
        self.install_screen(SendSMSScreen(), name = "send_test_sms")
        self.install_screen(SendSMSConfirmationScreen(participant_id=None, custom_message=None, premade_button_text=None), name = "send_sms_confirmation")
        
        self.push_screen("menu")

app = MainGUI()

def main():
    app.run()

if __name__ == "__main__":
    main()