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
        elif button_id == "initialize_button":
            self.app.push_screen("initialize_credentials")
        elif button_id == "send_test_sms_button":
            self.app.push_screen("send_test_sms")

    # This is the main menu screen of the application (will include buttons for different functionalities)
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        yield Footer()
        yield Static("Project Insight - Main Menu", id="menu_title")

        yield Button("Initialize Credentials", id="initialize_button")
        
        yield HorizontalGroup(
            Button("Add User to SMS Database", id="start_button"),
            Button("View User in SMS Database", id="view_button"),
            Button("Edit User in SMS Database", id="edit_button"),
            Button("Delete User from SMS Database", id="delete_button"),
            Button("Generate Report/Check Compliance", id="report_button", disabled = True), #TODO Implement this functionality
            Button("Send Test SMS or Manually Send Survey", id="send_test_sms_button"),
            Button("Exit", id="exit_button")
        )
