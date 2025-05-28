from textual.app import ComposeResult
from textual.screen import Screen
from textual.validation import Function
from textual.widgets import Input, Label, Button, Header
from textual.containers import HorizontalGroup
from methods.dynamoDB_methods import get_item_from_dynamodb


class ViewUserScreen(Screen):
    CSS_PATH = "view_user_screen.tcss"
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        yield Label("Enter the Participant ID to view user details:", id="view_user_label")
        yield Input(placeholder="Participant ID", id="participant_id_input", type='integer')
        yield Label("", id="user_details_label")  # Placeholder for user details
        yield HorizontalGroup (
            Button("View User", id="view_user_button"),
            Button("Back", id="back_button"),
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "view_user_button":
            participant_id = self.query_one("#participant_id_input").value
            user_details_label = self.query_one("#user_details_label", Label)
            # Implement logic to view user details
            user_data = get_item_from_dynamodb(participant_id)
            # Show user data in a suitable format
            if user_data:
                details = f"Participant ID: {user_data['participant_id']}\n" \
                          f"Study Start Date: {user_data['study_start_date']}\n" \
                          f"Study End Date: {user_data['study_end_date']}\n" \
                          f"Phone Number: {user_data['phone_number']}\n" \
                          f"Schedule Type: {user_data['schedule_type']}\n" \
                          f"Leaderboard Link: {user_data['lb_link']}"
                user_details_label.update(details)
            else:
                user_details_label.update("User not found.")
        elif event.button.id == "back_button":
            self.app.pop_screen()