from textual.app import ComposeResult
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.widgets import Button, Footer, Header, Label, DataTable
from ..methods.dynamoDB_methods import add_item_to_dynamodb
from textual.containers import VerticalGroup, HorizontalGroup
from ..elements.success_screen import SuccessScreen
import datetime

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
    
    # Calculate the phase breakdown based on the study start date (first phase is 4 days, second phase is 8 days, third phase is 2 days)
    def calculate_phase_breakdown(self) -> str: #TODO Fix this based on the actual dates we need
        start_date = datetime.datetime.strptime(self.study_start_date, "%Y-%m-%d")
        phase_1_end = start_date + datetime.timedelta(days=3)
        phase_2_end = phase_1_end + datetime.timedelta(days=8)
        phase_3_end = phase_2_end + datetime.timedelta(days=2)

        return phase_1_end.strftime("%Y-%m-%d"), phase_2_end.strftime("%Y-%m-%d"), phase_3_end.strftime("%Y-%m-%d")
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)  # Show the clock in the header
        # Display the user details that are about to be added
        yield VerticalGroup(
            Label(f"User Details:", id="user_details_title"),
            Label(f"Participant ID: {self.participant_id}"),  # Placeholder for participant ID
            Label(f"Study Start Date: {self.study_start_date}"),  # Placeholder for study start date 
            Label(f"Study End Date: {self.study_end_date}"),  # Placeholder for study end date
            Label(f"Phone Number: {self.phone_number}"),
            Label(f"Schedule Type: {self.schedule_type}"), #TODO Add what the schedule means
            Label(f"Leaderboard Link: {self.lb_link}"),
            id="user_details"
        )
        
        # Create DataTable without rows parameter
        yield VerticalGroup(
            DataTable(id="phase_breakdown_table"),
            DataTable(id="sms_schedule_table"),
            id="tables_container"
        )
        yield Label("Are you sure you want to add this user?", id = "confirmation_message")
        yield Grid(
            Button(label="Cancel", id="cancel_button"),
            Button(label="Confirm", id="confirm_button"),
            id="confirmation_buttons"
        )
        yield Footer()

    def on_mount(self) -> None:
        """Populate the DataTable after the widget is mounted."""
        table = self.query_one("#phase_breakdown_table", DataTable)
        
        # Add columns
        table.add_columns("Phase", "Start Date", "End Date")
        
        # Calculate phase breakdown and add rows
        phase_1_end, phase_2_end, phase_3_end = self.calculate_phase_breakdown()
        
        # Convert string dates back to datetime objects for calculation
        phase_1_end_date = datetime.datetime.strptime(phase_1_end, "%Y-%m-%d")
        phase_2_end_date = datetime.datetime.strptime(phase_2_end, "%Y-%m-%d")
        
        table.add_rows([
            ("Phase 1", self.study_start_date, phase_1_end),
            ("Phase 2", (phase_1_end_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), phase_2_end),
            ("Phase 3", (phase_2_end_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"), phase_3_end),
        ])
        
        # Populate the SMS schedule table
        sms_table = self.query_one("#sms_schedule_table", DataTable)
        sms_table.add_columns("Survey #1 Send Time", "Survey #2 Send Time", "Survey #3 Send Time", "Survey #4 Send Time")
        
        if self.schedule_type == "Early Bird Schedule":
            sms_table.add_rows([
                ("7:49 AM - 08:19 AM", "12:04 PM - 12:34 PM", "4:34 PM - 5:04 PM", "8:36 PM - 9:06 PM")
            ])
        elif self.schedule_type == "Standard Schedule":
            sms_table.add_rows([
                ("09:56 AM - 10:26 AM", "2:40 PM - 3:10 PM", "6:28 PM - 6:58 PM", "9:58 PM - 10:28 PM")
            ])
        elif self.schedule_type == "Night Owl Schedule":
            sms_table.add_rows([
                ("11:34 AM - 12:04 PM", "3:18 PM - 3:48 PM", "7:14 PM - 7:44 PM", "11:12 PM - 11:42 PM")
            ])

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm_button":
            add_item_to_dynamodb(self.participant_id, self.study_start_date, self.study_end_date, self.phone_number, self.schedule_type, self.lb_link)
            self.app.push_screen(SuccessScreen())
        elif event.button.id == "cancel_button":
            self.app.pop_screen()  # Close the confirmation screen