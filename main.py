from API_Service import FC26_API
from Send_To_Sheets import GoogleSheetsPayload
from Match_Parser import PlayerStatsParser, MatchPlayerParser
import pandas as pd
import time
 
# Every 2 mins check for new matches, if there is a new match, send the stats to google sheets
def main():
    print("Starting Pro Clubs Stats Tracker...")
    api = FC26_API()
    parser = PlayerStatsParser()#"newest_match.json"
    match_parser = MatchPlayerParser("club_matches.csv")
    print("Initialization complete. Entering main loop...")

    while True:
        print("\nChecking for new matches...")
        club_id = "3439844"
        
        # Fetch matches from API
        matches_df = api.get_club_matches_normalized(club_id, match_type="leagueMatch")
        
        # Save the fetched matches to CSV
        if matches_df is not None and not matches_df.empty:
            api.export_dataframe(matches_df, "club_matches.csv", file_format="csv")
            print("✓ Updated club_matches.csv with latest API data")
        else:
            print("✗ No matches returned from API")
            
        
        # Parse the CSV file to populate match_parser.matches
        match_parser.parse_csv()
        
        # Check if there are new matches
        if match_parser.check_for_new_matches():
            print("New match found! Parsing stats...")
            
            # Update and export FIRST so parse_stats() reads fresh data
            match_parser.update_most_recent_matches()
            match_parser.export_most_recent_match_to_json("newest_match.json")
            
            payloads = parser.parse_stats()
            for payload in payloads:
                sender = GoogleSheetsPayload(payload)
                response = sender.send()
                print(f"Sent payload: {payload} | Response: {response.text}")
        
        time.sleep(30)  # Sleep for 60 seconds before checking again

if __name__ == "__main__":
    main()