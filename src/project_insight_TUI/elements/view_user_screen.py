from textual.app import ComposeResult
from textual.screen import Screen
from textual.validation import Function
from textual.widgets import Input, Label, Button, Header
from textual.containers import HorizontalGroup
from ..methods.dynamoDB_methods import get_item_from_dynamodb


class ViewUserScreen(Screen):
    CSS_PATH = "view_user_screen.tcss"
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        yield Label("Enter the Participant ID to view user details:", id="view_user_label")
        yield Input(placeholder="Participant ID", id="participant_id_input", type='integer')
        yield Label("", id="user_details_label")  # Placeholder for user details
        yield HorizontalGroup (
            Button("Back", id="back_button"),
            Button("View User", id="view_user_button"),
            id="buttons_group"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "view_user_button":
            participant_id = self.query_one("#participant_id_input", Input).value
            user_details_label = self.query_one("#user_details_label", Label)
            
            # Check if participant_id is empty
            if not participant_id or participant_id.strip() == "":
                user_details_label.update("Please enter a Participant ID.")
                return
            
            try:
                # Implement logic to view user details
                user_data = get_item_from_dynamodb(participant_id)
                
                # Simplified logic - just check None first
                if user_data is None:
                    user_details_label.update("User Not Found.")
                    return  # Exit early to avoid any other updates
                
                # If we get here, user_data is not None
                if user_data == {}:
                    user_details_label.update("User Not Found (empty response).")
                    return
                
                # Show user data
                details = f"Participant ID: {user_data['participant_id']}\n" \
                          f"Study Start Date: {user_data.get('study_start_date', 'N/A')}\n" \
                          f"Study End Date: {user_data.get('study_end_date', 'N/A')}\n" \
                          f"Phone Number: {user_data.get('phone_number', 'N/A')}\n" \
                          f"Schedule Type: {user_data.get('schedule_type', 'N/A')}\n" \
                          f"Leaderboard Link: {user_data.get('lb_link', 'N/A')}"
                user_details_label.update(details)
                
            except Exception as e:
                user_details_label.update(f"Error retrieving user data: {str(e)}")
                
        elif event.button.id == "back_button":
            self.app.pop_screen()