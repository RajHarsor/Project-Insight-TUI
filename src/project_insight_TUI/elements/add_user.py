from textual.app import ComposeResult
from textual.validation import Function
from textual.screen import Screen
from textual.widgets import Button, Static, Input, Pretty, Select, Header
from textual.containers import VerticalGroup, HorizontalGroup, Container
from textual import on
from ..methods.dynamoDB_methods import add_item_to_dynamodb
from ..elements.confirm_add_user import ConfirmAddUserScreen

class AddUserScreen(Screen):
    CSS_PATH = "add_user_screen.tcss"  # Path to the CSS file for styling
    

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id

        if button_id == "submit_button":
            participant_id = self.query_one("#participant_id_input").value
            study_start_date = self.query_one("#study_start_date_input").value
            study_end_date = self.query_one("#study_end_date_input").value
            phone_number = self.query_one("#phone_number_input").value
            schedule_type = self.query_one("#schedule_select").value
            lb_link = self.query_one("#lb_link_input").value
            # confirm add_user screen
            self.app.push_screen(ConfirmAddUserScreen(participant_id=participant_id,
                                                        study_start_date=study_start_date,
                                                        study_end_date=study_end_date,
                                                        phone_number=phone_number,
                                                        schedule_type=schedule_type,
                                                        lb_link=lb_link))

        if button_id == "back_button":
            self.app.pop_screen()
            self.app.push_screen("menu")
        
        if button_id == "back-button":
            self.app.pop_screen()


    def compose(self) -> ComposeResult:
        
            # Schedule Options
        SCHEDULE_OPTIONS = """Early Bird Schedule
Standard Schedule
Night Owl Schedule""".splitlines()

        yield Header(show_clock=True)  # Show the clock in the header
        yield Static("Add User to SMS Database", id="add_user_title")
        yield VerticalGroup(
            Input(placeholder="Enter Participant ID", id="participant_id_input", type='integer'),
            Input(placeholder="Enter Study Start Date (YYYY-MM-DD)", id="study_start_date_input", type='text', validators=[Function(lambda x: len(x) == 10 and x[4] == '-' and x[7] == '-', "Date must be in YYYY-MM-DD format")]),
            Input(placeholder="Enter Study End Date (YYYY-MM-DD)", id="study_end_date_input", type='text', validators=[Function(lambda x: len(x) == 10 and x[4] == '-' and x[7] == '-', "Date must be in YYYY-MM-DD format")]),
            Input(placeholder="Enter Phone Number (+1XXXXXXXXXX)", id="phone_number_input", type='text', validators=[Function(lambda x: len(x) == 12 and x[0:2] == '+1' and x[2:].isdigit(), "Phone number must be in +1XXXXXXXXXX format")]),
            Input(placeholder="Enter Leaderboard Link", id="lb_link_input", type='text', validators=[Function(lambda x: len(x) > 0, "LB Link cannot be empty")]),
            Select(((line, line) for line in SCHEDULE_OPTIONS), allow_blank=True, id="schedule_select", prompt="Select Schedule"),
        )
        yield Container(
            Pretty("", id="validation_result"),
            id="validation_wrapper",
        )
        yield HorizontalGroup(
            Button("Back", id="back_button"),
            Button("Submit", id="submit_button"),
            id="navigation_buttons"
        )

    @on(Input.Changed)
    def show_invalid_reasons(self, event: Input.Changed) -> None:
        # Updating the UI to show the reasons why validation failed
        if not event.validation_result.is_valid:
            self.query_one(Pretty).update(event.validation_result.failure_descriptions)
        else:
            self.query_one(Pretty).update(["All inputs are valid."])