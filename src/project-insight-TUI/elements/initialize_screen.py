from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Label, Static, Header
from textual.containers import VerticalGroup, HorizontalGroup
from textual.widgets import Button
from methods.initialize_methods import check_env_file_exists, check_env_variables  # Import the method to check for .env file existence

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
            Button("Go to step 2 (No .env file)", id="next-step-button", disabled=True),  # Initially disabled
            Button("Go to step 2 (Incomplete .env file)", id="next-step-button-incomplete", disabled=True),
            id="button-panel"
        )
    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "main-menu-button":
            self.app.push_screen("menu")
        elif event.button.id == "submit-button":
            if check_env_file_exists() and check_env_variables():
                # If true, update the label to indicate success and enable the next step button
                self.query_one("#step1_result", Label).update("Result: Found .env file with all the required variables. Go back to the main menu.")
            elif not check_env_variables():
                self.query_one("#step1_result", Label).update("Result: Found .env file but missing required variables. Go to step 2 (Incomplete .env file).")
                # Enable the next step button for incomplete .env file
                self.query_one("#next-step-button-incomplete", Button).disabled = False
            else:
                self.query_one("#step1_result", Label).update("Result: No .env file found. Go to step 2 (no .env file).")
                # Enable the next step button regardless of the result
                self.query_one("#next-step-button", Button).disabled = False