from textual.app import ComposeResult
from textual.screen import ModalScreen
from textual.widgets import Label, Static, Header, Button, Select, Input
from textual.containers import VerticalGroup, HorizontalGroup, Container
from ..methods.initialize_methods import check_incomplete_env_file, update_env_variable
from ..elements.menu_screen import MenuScreen

LINES= """aws_access_key_id
aws_secret_access_key
region
table_name""".splitlines()

class InitializeIncompleteCredentialsScreen(ModalScreen):
    CSS_PATH = "initialize_incomplete_credentials_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        missing_vars = check_incomplete_env_file()

        # Display the missing environment variables
        missing_vars_text = "Missing environment variables:\n" + "\n".join(missing_vars) if missing_vars else "All required environment variables are set."
        yield Label(missing_vars_text, id="missing_vars_label")
        
        yield HorizontalGroup(
            Select(((line, line) for line in LINES), id="variable_select", prompt="Select a variable to update"),
            Input(placeholder="Enter the value for the selected variable", id="variable_value_input", type='text'),
            id="variable_input_group"
        )
        yield Label("", id="update_status_label")
        
        yield HorizontalGroup(
            Button("Go Back", id="back_to_menu_button"),
            Button("Back to Main Menu", id="main-menu-button"),
            Button("Update Variable", id="update_variable_button"),
            id="action_buttons"
        )
        
    def on_button_pressed(self, event) -> None:
        button_id = event.button.id

        if button_id == "back_to_menu_button":
            self.app.pop_screen()
        elif button_id == "main-menu-button":
            self.app.push_screen(MenuScreen())
        elif button_id == "update_variable_button":
            selected_variable = self.query_one(Select).value
            variable_value = self.query_one("#variable_value_input", Input).value
            
            if selected_variable and variable_value:
                update_env_variable(selected_variable, variable_value)
                self.query_one("#update_status_label", Label).update(f"Updated {selected_variable} successfully.")
            else:
                self.query_one("#update_status_label", Label).update("Please select a variable and enter a value.")
