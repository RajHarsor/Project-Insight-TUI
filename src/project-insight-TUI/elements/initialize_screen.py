from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Label, Static, Header, Button
from textual.containers import VerticalGroup, HorizontalGroup
from methods.initialize_methods import check_env_file_exists, check_env_variables  # Import the method to check for .env file existence
from elements.initialize_no_env_file_screen import InitializeNoEnvFileScreen  # Import the screen for incomplete .env file handling
from elements.initialize_incomplete_credentials_screen import InitializeIncompleteCredentialsScreen  # Import the screen for incomplete .env file handling
from elements.update_env_file_screen import UpdateEnvFileScreen  # Import the screen for updating the .env file

class InitializeCredentialsScreen(Screen):

    CSS_PATH = "initialize_screen.tcss"  # Path to the CSS file for styling

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        yield Label("Initialize Credentials", id="initialize_title")
        yield Static("Step 1: Check if you have a .env file in the root directory.", id="step1")
        yield Label("", id="step1_result")
        yield HorizontalGroup(
            Button("Main Menu", id="main-menu-button"),
            Button("Check .env", id="submit-button"),
            Button("Update .env File", id="update-env-button"), #TODO: Write this method
            Button("Go to step 2 (No .env file)", id="next-step-button", disabled=True),  # Initially disabled
            Button("Go to step 2 (Incomplete .env file)", id="next-step-button-incomplete", disabled=True),
            id="button-panel"
        )
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "main-menu-button":
            self.app.push_screen("menu")
        elif event.button.id == "submit-button":
            exists = check_env_file_exists()  # Check if .env file exists
            var_exists = check_env_variables()  # Check if required environment variables are set
            if exists == True and var_exists == True:
                # If true, update the label to indicate success and enable the next step button
                self.query_one("#step1_result", Label).update("Result: Found .env file with all the required variables. Go back to the main menu or update credentials.")
            elif exists == True and var_exists == False:
                self.query_one("#step1_result", Label).update("Result: Found .env file but missing required variables. Go to step 2 (Incomplete .env file).")
                # Enable the next step button for incomplete .env file
                self.query_one("#next-step-button-incomplete", Button).disabled = False
            else:
                self.query_one("#step1_result", Label).update("Result: No .env file found. Go to step 2 (no .env file).")
                # Enable the next step button regardless of the result
                self.query_one("#next-step-button", Button).disabled = False

        if event.button.id == "next-step-button":
            # Navigate to the next step for no .env file
            self.app.push_screen(InitializeNoEnvFileScreen())
        elif event.button.id == "next-step-button-incomplete":
            # Navigate to the next step for incomplete .env file
            self.app.push_screen(InitializeIncompleteCredentialsScreen())  # This should be the screen for handling incomplete .env files
        elif event.button.id == "update-env-button":
            # Handle updating the .env file
            self.app.push_screen(UpdateEnvFileScreen())