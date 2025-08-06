from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import HorizontalGroup
from textual.widgets import Label, Button, Footer, Header, Input
from ..elements.menu_screen import MenuScreen
from ..methods.dynamoDB_methods import get_item_from_dynamodb

class CheckIndividualComplianceScreen(Screen):
    CSS_PATH = "check_individual_compliance_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("Please select a user to check compliance:", id="compliance_message")
        yield Input(placeholder="Enter participant ID", id="user_input")
        yield Label("", id="start_end_date")
        yield Label("", id="compliance_result")
        #TODO Add datatable for days 1-14 of study
        yield HorizontalGroup(
            Button(label="Check Compliance", id="check_compliance_button"),
            Button(label="Back to Main Menu", id="main_menu_button"),
            id="action_buttons"
        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "check_compliance_button":
            # Query the input field for the participant ID
            participant_id = self.query_one("#user_input", Input).value
            user_data = get_item_from_dynamodb(participant_id)
            if user_data:
                start_date = user_data.get("study_start_date", "N/A")
                end_date = user_data.get("study_end_date", "N/A")
                self.query_one("#start_end_date", Label).update(f"Study Start Date: {start_date} | Study End Date: {end_date}")
        elif event.button.id == "main_menu_button":
            self.app.push_screen(MenuScreen())