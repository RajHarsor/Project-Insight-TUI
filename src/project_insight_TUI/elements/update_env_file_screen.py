from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Label, Button, Header, Input, Select
from ..methods.initialize_methods import get_env_variables, update_or_create_env_var
from textual.containers import HorizontalGroup
from ..elements.menu_screen import MenuScreen  # Import the main menu screen
from textual import on
from tkinter import filedialog

LINES = """aws_access_key_id
aws_secret_access_key
region
table_name
qualtrics_survey_1a_path
qualtrics_survey_1b_path
qualtrics_survey_2_path
qualtrics_survey_3_path
qualtrics_survey_4_path
participant_db_path""".splitlines()

CREDENTIAL_VARS = ["aws_access_key_id", "aws_secret_access_key", "region", "table_name"]
PATH_VARS = ["qualtrics_survey_1a_path", "qualtrics_survey_1b_path", "qualtrics_survey_2_path", 
            "qualtrics_survey_3_path", "qualtrics_survey_4_path", "participant_db_path"]

class UpdateEnvFileScreen(Screen):
    CSS_PATH = "update_env_file_screen.tcss"  # Path to the CSS file for styling

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        yield Label("Update Environment Variables", id="update_title")
        yield HorizontalGroup(
            Label("", id="current_env_vars"),
            Label("", id="update_result"),
            id="env_vars_display"
        )
        yield HorizontalGroup(
            Select(((line, line) for line in LINES), id="env_var_select", prompt="Select an environment variable to update"),
            Input(placeholder="Enter new value", id="new_value_input", disabled=True),  # Initially disabled, will be enabled based on selection
            Button("Choose Path", id="choose_path_button", disabled=True),  # Initially disabled, will be enabled based on selection
            id="variable_input_group"
        )
        yield HorizontalGroup(
            Button("Update Variable", id="update_variable_button", disabled=True),
            id="update_button_group"
        )
        yield HorizontalGroup(
            Button("Go Back", id="go-back-button", disabled=True),
            Button("Main Menu", id="main-menu-button"),
            id="button-panel"
        )
    
    def on_mount(self) -> None:
        # Display current environment variables in the label
        self.env_vars = get_env_variables()
        env_vars_str = "\n".join(f"{key}={value}" for key, value in self.env_vars.items())
        self.query_one("#current_env_vars", Label).update(env_vars_str)
    
    @on(Select.Changed)
    def on_select_changed(self, event: Select.Changed) -> None:
        """Handle changes in the select dropdown"""
        selected_var = event.value
        
        input_widget = self.query_one("#new_value_input", Input)
        path_button = self.query_one("#choose_path_button", Button)
        update_button = self.query_one("#update_variable_button", Button)
        
        if selected_var in CREDENTIAL_VARS:
            # Show input box, hide choose path button
            input_widget.disabled = False
            input_widget.display = True
            path_button.disabled = True
            path_button.display = False
            update_button.disabled = False  # Enable the update button
        elif selected_var in PATH_VARS:
            # Hide input box, show choose path button
            input_widget.disabled = True
            input_widget.display = False
            path_button.disabled = False
            path_button.display = True
            update_button.disabled = False  # Enable the update button
        else:
            # No variable selected, disable both
            input_widget.disabled = True
            input_widget.display = False
            path_button.disabled = True
            path_button.display = False
        
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "go-back-button":
            self.app.pop_screen()
        elif event.button.id == "main-menu-button":
            self.app.push_screen(MenuScreen())
        elif event.button.id == "choose_path_button":
            # Open file dialog to select a path
            path = filedialog.askopenfilename(title="Select Path")
            if path:
                input_widget = self.query_one("#new_value_input", Input)
                input_widget.value = path
                input_widget.disabled = False
        elif event.button.id == "update_variable_button":
            selected_var = self.query_one("#env_var_select", Select).value
            new_value = self.query_one("#new_value_input", Input).value
            if selected_var and new_value:
                update_or_create_env_var(self.env_vars,selected_var, new_value)
                # Update the display of update_result to get_new_env_variables
                updated_env_vars = get_env_variables()
                self.query_one("#update_result", Label).update("\n".join(f"{key}={value}" for key, value in updated_env_vars.items()))