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

class MainGUI(App):
    TITLE = "Project Insight GUI"
    def on_mount(self) -> None:
        self.theme = 'nord'
        self.install_screen(MenuScreen(), name = "menu")
        self.install_screen(AddUserScreen(), name = "add_user")
        self.install_screen(ConfirmAddUserScreen(), name = "confirm_add_user")
        self.install_screen(SuccessScreen(), name = "success")
        self.install_screen(ViewUserScreen(), name = "view_user")
        self.install_screen(EditUserScreen(), name = "edit_user")
        self.install_screen(DeleteUserScreen(), name = "delete_user")
        self.install_screen(InitializeCredentialsScreen(), name = "initialize_credentials")
        self.install_screen(SendSMSScreen(), name = "send_test_sms")  
        self.push_screen("menu")

app = MainGUI()

def main():
    app.run()

if __name__ == "__main__":
    main()