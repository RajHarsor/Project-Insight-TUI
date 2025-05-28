from textual.app import ComposeResult
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.widgets import Button, Footer, Header, Label
from methods.dynamoDB_methods import add_item_to_dynamodb
from textual.containers import VerticalGroup
from elements.success_screen import SuccessScreen

class ConfirmAddUserScreen(ModalScreen):
    def __init__(
        self,
        participant_id: str,
        study_start_date: str,
        study_end_date: str,
        phone_number: str,
        schedule_type: str,
        lb_link: str,
        name: str | None = None,
        id: str | None = None,
        classes: str | None = None,
    ) -> None:
        super().__init__(name=name, id=id, classes=classes)
        self.participant_id = participant_id
        self.study_start_date = study_start_date
        self.study_end_date = study_end_date
        self.phone_number = phone_number
        self.schedule_type = schedule_type
        self.lb_link = lb_link

    CSS_PATH = "confirm_add_user_screen.tcss"  # Path to the CSS file for styling

    def compose(self) -> ComposeResult:
        yield Header()
        # Display the user details that are about to be added
        yield VerticalGroup(
            Label(f"User Details:"),
            Label(f"Participant ID: {self.participant_id}"),  # Placeholder for participant ID
            Label(f"Study Start Date: {self.study_start_date}"),  # Placeholder for study start date
            Label(f"Study End Date: {self.study_end_date}"),  # Placeholder for study end date
            Label(f"Phone Number: {self.phone_number}"),
            Label(f"Schedule Type: {self.schedule_type}"),
            Label(f"Leaderboard Link: {self.lb_link}"),
            id = "user_details"
        )
        yield Grid(
            Label("Are you sure you want to add this user?"),
        )
        yield Grid(
            Button(label="Confirm", id="confirm_button"),
            Button(label="Cancel", id="cancel_button"),
            id="confirmation_buttons"
        )
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm_button":
            add_item_to_dynamodb(self.participant_id, self.study_start_date, self.study_end_date, self.phone_number, self.schedule_type, self.lb_link)
            self.app.push_screen(SuccessScreen())
        elif event.button.id == "cancel_button":
            self.app.pop_screen()  # Close the confirmation screen