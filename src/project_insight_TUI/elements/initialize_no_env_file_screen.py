from textual.app import ComposeResult
from textual.screen import ModalScreen
from textual.widgets import Label, Static, Header, Button
from textual.containers import VerticalGroup, HorizontalGroup, Container
from methods.initialize_methods import create_env_file
from textual.widgets import Input
from tkinter import filedialog

class InitializeNoEnvFileScreen(ModalScreen):
    CSS_PATH = "initialize_no_env_file_screen.tcss"
    
    def __init__(self):
        super().__init__()
        # Store the survey paths as instance variables
        self.qualtrics_survey_1a_path = None
        self.qualtrics_survey_1b_path = None
        self.qualtrics_survey_2_path = None
        self.qualtrics_survey_3_path = None
        self.qualtrics_survey_4_path = None
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True) 
        yield VerticalGroup(
            Input(placeholder="Enter your AWS Access Key ID", id="aws_access_key_id_input", type='text'),
            Input(placeholder="Enter your AWS Secret Access Key", id="aws_secret_access_key_input", type='text'),
            Input(placeholder="Enter your DynamoDB Table Name", id="table_name_input", type='text'),
            id="credentials_input_group"
        )
        # Buttons for optional Qualtrics survey paths
        yield HorizontalGroup(
            Button("Add Qualtrics Survey 1A Path (Optional)", id="add_survey_1a_button"),
            Button("Add Qualtrics Survey 1B Path (Optional)", id="add_survey_1b_button"),
            Button("Add Qualtrics Survey 2 Path (Optional)", id="add_survey_2_button"),
            Button("Add Qualtrics Survey 3 Path (Optional)", id="add_survey_3_button"),
            Button("Add Qualtrics Survey 4 Path (Optional)", id="add_survey_4_button"),
            id="survey_path_buttons"
        )
        yield VerticalGroup(
            Label("Place", id="survey_paths_status"),
            Label("Place", id="success_message"),
            id="status_labels"
        )
        yield HorizontalGroup(
            Button("Go Back", id="back_to_menu_button"),
            Button("Create .env File", id="create_env_file_button"),
            id="action_buttons"
        )

    def on_button_pressed(self, event) -> None:
        button_id = event.button.id

        # Handle survey path buttons
        if button_id == "add_survey_1a_button":
            path = filedialog.askopenfilename(title="Select Qualtrics Survey 1A Path")
            if path:
                self.qualtrics_survey_1a_path = path
                self.update_survey_status("Survey 1A path selected")
        elif button_id == "add_survey_1b_button":
            path = filedialog.askopenfilename(title="Select Qualtrics Survey 1B Path")
            if path:
                self.qualtrics_survey_1b_path = path
                self.update_survey_status("Survey 1B path selected")
        elif button_id == "add_survey_2_button":
            path = filedialog.askopenfilename(title="Select Qualtrics Survey 2 Path")
            if path:
                self.qualtrics_survey_2_path = path
                self.update_survey_status("Survey 2 path selected")
        elif button_id == "add_survey_3_button":
            path = filedialog.askopenfilename(title="Select Qualtrics Survey 3 Path")
            if path:
                self.qualtrics_survey_3_path = path
                self.update_survey_status("Survey 3 path selected")
        elif button_id == "add_survey_4_button":
            path = filedialog.askopenfilename(title="Select Qualtrics Survey 4 Path")
            if path:
                self.qualtrics_survey_4_path = path
                self.update_survey_status("Survey 4 path selected")
        elif button_id == "create_env_file_button":
            aws_access_key_id = self.query_one("#aws_access_key_id_input").value
            aws_secret_access_key = self.query_one("#aws_secret_access_key_input").value
            table_name = self.query_one("#table_name_input").value

            # Create the .env file with the provided values and stored paths
            create_env_file(aws_access_key_id, aws_secret_access_key, table_name,
                            self.qualtrics_survey_1a_path, self.qualtrics_survey_1b_path,
                            self.qualtrics_survey_2_path, self.qualtrics_survey_3_path,
                            self.qualtrics_survey_4_path)
            self.query_one("#success_message", Label).update("Success: .env file created with the provided credentials.")
        elif button_id == "back_to_menu_button":
            self.app.pop_screen()
    
    def update_survey_status(self, message: str) -> None:
        """Update the survey paths status label"""
        self.query_one("#survey_paths_status", Label).update(message)