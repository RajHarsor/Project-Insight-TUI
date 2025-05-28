from textual.app import ComposeResult
from textual.screen import Screen
from textual.validation import Function
from textual.widgets import Input, Label, Button, Select, Header
from textual.containers import HorizontalGroup, Grid
from methods.dynamoDB_methods import get_item_from_dynamodb, update_item_in_dynamodb

class EditUserScreen(Screen):
    CSS_PATH = "edit_user_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        yield Label("Enter the Participant ID to edit user details:", id="edit_user_label")
        yield Input(placeholder="Participant ID", id="participant_id_input", type='integer')
        yield Grid(
            # Grid for displaying user details
            Label("", id='user_details_label'),
            Select(prompt="Select Field to Edit", id="field_select", options=[
            ("study_start_date", "study_start_date"),
            ("study_end_date", "study_end_date"),
            ("phone_number", "phone_number"),
            ("schedule_type", "schedule_type"),
            ("lb_link", "lb_link"),
        ]), Input(placeholder="New Value", id="new_value_input"),
            id="user_details_grid",
        )
        yield Label("", id='new_details_label')  # Placeholder for updated user details
        yield HorizontalGroup(
            Button("Back", id="back_button"),
            Button("View User", id="view_user_button"),
            Button("Update User", id="update_user_button", disabled=True)
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "view_user_button":
            participant_id = self.query_one("#participant_id_input").value
            user_data = get_item_from_dynamodb(participant_id)
            user_details_label = self.query_one("#user_details_label", Label)
            #print(user_data)
            if user_data:
                details =  "Current User Details:\n" \
                    f"Participant ID: {user_data['participant_id']}\n" \
                    f"Study Start Date: {user_data['study_start_date']}\n" \
                    f"Study End Date: {user_data['study_end_date']}\n" \
                    f"Phone Number: {user_data['phone_number']}\n" \
                    f"Schedule Type: {user_data['schedule_type']}\n" \
                    f"Leaderboard Link: {user_data['lb_link']}"
                user_details_label.update(details)
                # Show the field select dropdown
                field_select = self.query_one("#field_select", Select)
                field_select.styles.display = "block"
                # Show the new value input field
                new_value_input = self.query_one("#new_value_input", Input)
                new_value_input.styles.display = "block"
                # Enable the update button
                update_button = self.query_one("#update_user_button", Button)
                update_button.disabled = False
            else:
                user_details_label.update("User not found.")
                
        if event.button.id == "update_user_button":
            participant_id = self.query_one("#participant_id_input").value
            field_select = self.query_one("#field_select", Select)
            new_value_input = self.query_one("#new_value_input", Input)
            new_details_label = self.query_one("#new_details_label", Label)

            selected_field = field_select.value
            new_value = new_value_input.value

            # Update the user in DynamoDB
            update_item_in_dynamodb(participant_id, selected_field, new_value)

            # Show confirmation message
            new_details_label.update(f"Updated {selected_field} to {new_value} for Participant ID {participant_id}")

        if event.button.id == "back_button":
            self.app.pop_screen()