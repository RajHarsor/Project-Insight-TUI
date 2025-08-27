from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import HorizontalGroup
from textual.widgets import Label, Button, Footer, Header, Input, DataTable
from ..elements.menu_screen import MenuScreen
from ..methods.dynamoDB_methods import get_item_from_dynamodb
import datetime
from ..methods.compliance_methods import generate_compliance_tables

# First column should be date_range that we calculated

compliance_key_rows = [
    ("Key", "Description"),
    ("✓ SR", "Single Response for day on Time"),
    ("✗ SR", "Single Response for day but Late"),
    ("✓ MR", "Multiple Responses for day but one on Time"),
    ("✗ MR", "Multiple Responses for day but none on Time"),
    ("NR", "No Response on this survey for day")
]


class CheckIndividualComplianceScreen(Screen):
    CSS_PATH = "check_individual_compliance_screen.tcss"

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Label("Please select a user to check compliance:", id="compliance_message")
        yield Input(placeholder="Enter participant ID", id="user_input")
        yield Label("", id="start_end_date")
        yield Label("", id="compliance_result")
        # Create Datatable with 14 columns and 5 rows
        yield Label("Send Times:", id="send_times_label")
        yield HorizontalGroup(
            DataTable(id="send_times_table"),
            id="send_times_group"
        )
        yield Label("", id="compliance_data_label")
        yield HorizontalGroup(
            DataTable(id="compliance_data_table"),
            DataTable(id='compliance_key'),
            id="compliance_tables"
        )
        
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
                compliance_rows, send_time_rows, ID = generate_compliance_tables(participant_id)
                self.query_one("#start_end_date", Label).update(f"Study Start Date: {start_date} | Study End Date: {end_date} | Participant Initials: {ID}")
                print(compliance_rows, send_time_rows)
                # Convert start_date and end_date into dateTime objects
                try:
                    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d")
                    end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d")
                except ValueError:
                    self.query_one("#compliance_result", Label).update("Invalid date format in user data.")
                    return
                
                # Table of send_time_rows
                data_table = self.query_one("#send_times_table", DataTable)
                data_table.add_columns(*send_time_rows[0])
                for row in send_time_rows[1:]:
                    data_table.add_row(*row)
                
                data_table2 = self.query_one("#compliance_data_table", DataTable)
                data_table2.add_columns(*compliance_rows[0])
                for row in compliance_rows[1:]:
                    data_table2.add_row(*row)
                
                data_table3 = self.query_one("#compliance_key", DataTable)
                data_table3.add_columns(*compliance_key_rows[0])
                for row in compliance_key_rows[1:]:
                    data_table3.add_row(*row)
                
                # Show compliance data table since it's hidden by default
                data_table.styles.display = "block"  # Show the table
                data_table2.styles.display = "block"
                data_table3.styles.display = "block"
                self.query_one("#send_times_label", Label).update("Send Times:")
                self.query_one("#compliance_data_label", Label).update("Compliance Data:")
                
                
        elif event.button.id == "main_menu_button":
            self.app.push_screen(MenuScreen())