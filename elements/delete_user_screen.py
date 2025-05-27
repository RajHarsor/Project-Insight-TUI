from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Label, Input, Button
from textual.containers import HorizontalGroup
from methods.dynamoDB_methods import get_item_from_dynamodb, delete_item_from_dynamodb

class DeleteUserScreen(Screen):
    CSS_PATH = "delete_user_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Label("Enter the Participant ID to delete user:", id="delete_user_label")
        yield Input(placeholder="Participant ID", id="participant_id_input", type='integer')
        yield Label("", id='delete_user_message')  # Placeholder for delete message
        yield HorizontalGroup(
            Button("Delete User", id="delete_user_button"),
            Button("Back", id="back_button")
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "delete_user_button":
            participant_id = self.query_one("#participant_id_input").value
            user_data = get_item_from_dynamodb(participant_id)
            if user_data:
                delete_item_from_dynamodb(participant_id)
                self.query_one("#delete_user_message", Label).update("User deleted successfully.")
            else:
                self.query_one("#delete_user_message", Label).update("User not found.")

        if event.button.id == "back_button":
            self.app.pop_screen()  # Go back to the previous screen