from textual.app import ComposeResult, RenderResult
from textual.screen import Screen
from textual.widgets import Footer, Header, Button, Static
from textual.containers import HorizontalGroup

class MenuScreen(Screen):
    CSS_PATH = "menu_screen.tcss"  # Path to the CSS file for styling
    
    def render(self) -> RenderResult:
        return "Project Insight - Main Menu"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id

        if button_id == "start_button":
            self.app.push_screen("add_user")
        elif button_id == "edit_button":
            self.app.push_screen("edit_user")
        elif button_id == "delete_button":
            self.app.push_screen("delete_user")
        elif button_id == "view_button":
            self.app.push_screen("view_user")
        elif button_id == "report_button":
            self.app.push_screen("generate_report")
        elif button_id == "exit_button":
            self.app.exit()

    # This is the main menu screen of the application (will include buttons for different functionalities)
    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()
        yield Static("Project Insight - Main Menu", id="menu_title")

        #yield Button("Initialize Credentials (Only for first time use)", id="initialize_button")
        
        yield HorizontalGroup(
            Button("Add User to SMS Database", id="start_button"),
            Button("View User in SMS Database", id="view_button"),
            Button("Edit User in SMS Database", id="edit_button"),
            Button("Delete User from SMS Database", id="delete_button"),
            Button("Generate Report", id="report_button"), #TODO Implement this functionality
            Button("Send Test SMS", id="send_test_sms_button"), #TODO Implement this functionality
            Button("Exit", id="exit_button")
        )
