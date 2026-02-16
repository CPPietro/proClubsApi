
# Required libraries
import json
import pandas as pd
import ast
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

@dataclass
class PlayerStats:
    """Structured data for a player's match statistics"""
    player_id: str
    player_name: str
    club_id: str
    position: str
    archetype_id: str
    rating: float
    goals: int
    assists: int
    shots: int
    passes: int
    pass_attempts: int
    tackles: int
    tackle_attempts: int
    saves: int
    seconds_played: int
    score: int
    wins: int
    losses: int
    redcards: int
    user_result: int


@dataclass
class MatchData:
    """Structured data for a complete match"""
    match_id: str
    timestamp: str
    time_ago: str
    players_by_club: Dict[str, List[PlayerStats]]

    def get_players_for_club(self, club_id: str) -> List[PlayerStats]:
        """Get all players for a specific club in this match"""
        return self.players_by_club.get(club_id, [])
    
@dataclass
class MostRecentMatch:
    """Data class for storing the most recent match and its players"""
    match_id: str
    timestamp: str
    time_ago: str
    players: List[PlayerStats]
    is_new: bool = False
    
    def get_players_for_club(self, club_id: str) -> List[PlayerStats]:
        """Get all players for a specific club in the most recent match"""
        return [p for p in self.players if p.club_id == club_id]
    
class MatchPlayerParser:
    """Parse match data and extract player information into structured format"""
    
    def __init__(self, csv_filepath: str, most_recent_filepath: str = "most_recent_matches.csv"):
        self.csv_filepath = csv_filepath
        self.most_recent_filepath = most_recent_filepath
        self.matches: List[MatchData] = []
        self.most_recent_match: Optional[MostRecentMatch] = None
        self.new_matches_detected = False

    # Not needed, keeping just incase
    #def parse_csv(self) -> List[MatchData]:
    #    """Parse the CSV file and extract match data"""
    #    df = pd.read_csv(self.csv_filepath)
    #    
    #    for idx, row in df.iterrows():
    #        try:
    #            match = self._parse_match_row(row)
    #            self.matches.append(match)
    #        except Exception as e:
    #            print(f"Error parsing row {idx}: {e}")
    #            continue
        
    #    return self.matches

    def _parse_match_row(self, row) -> MatchData:
        """Parse a single CSV row into a MatchData object"""
        match_id = str(row['matchId'])
        timestamp = str(row['timestamp'])
        time_ago = str(row['timeAgo'])
        
        # Parse the players column (it's a Python dict string, not JSON)
        players_str = str(row['players'])
        try:
            players_data = ast.literal_eval(players_str)
        except (ValueError, SyntaxError) as e:
            raise ValueError(f"Could not parse players data: {e}")
        
        # Build players by club dictionary
        players_by_club = {}
        
        for club_id, club_players in players_data.items():
            players_by_club[club_id] = []
            
            for player_id, player_stats in club_players.items():
                player = PlayerStats(
                    player_id=player_id,
                    player_name=player_stats.get('playername', 'Unknown'),
                    club_id=club_id,
                    position=player_stats.get('pos', 'Unknown'),
                    archetype_id=player_stats.get('archetypeid', '0'),
                    rating=float(player_stats.get('rating', 0)),
                    goals=int(player_stats.get('goals', 0)),
                    assists=int(player_stats.get('assists', 0)),
                    shots=int(player_stats.get('shots', 0)),
                    passes=int(player_stats.get('passesmade', 0)),
                    pass_attempts=int(player_stats.get('passattempts', 0)),
                    tackles=int(player_stats.get('tacklesmade', 0)),
                    tackle_attempts=int(player_stats.get('tackleattempts', 0)),
                    saves=int(player_stats.get('saves', 0)),
                    seconds_played=int(player_stats.get('secondsPlayed', 0)),
                    score=int(player_stats.get('SCORE', 0)),
                    wins=int(player_stats.get('wins', 0)),
                    losses=int(player_stats.get('losses', 0)),
                    redcards=int(player_stats.get('redcards', 0)),
                    user_result=int(player_stats.get('userResult', 0)),
                )
                players_by_club[club_id].append(player)
        
        return MatchData(
            match_id=match_id,
            timestamp=timestamp,
            time_ago=time_ago,
            players_by_club=players_by_club
        )
    
    def load_most_recent_matches(self) -> Optional[pd.DataFrame]:
        """Load existing most_recent_matches CSV if it exists"""
        try:
            df = pd.read_csv(self.most_recent_filepath)
            return df
        except FileNotFoundError:
            return None
        
    def check_for_new_matches(self) -> bool:
        """
        Compare club_matches with most_recent_matches to detect new matches.
        Returns True if new matches are found.
        """
        if not self.matches:
            return False
        
        # Load existing most recent matches
        most_recent_df = self.load_most_recent_matches()
        
        if most_recent_df is None:
            print("✓ No existing most_recent_matches.csv found. All current matches are new.")
            self.new_matches_detected = True
            return True
        
        # Get match IDs from both dataframes
        existing_match_ids = set(most_recent_df['matchId'].astype(str))
        current_match_ids = set([m.match_id for m in self.matches])
        
        # Find new matches
        new_match_ids = current_match_ids - existing_match_ids
        
        if new_match_ids:
            print(f"✓ Detected {len(new_match_ids)} new match(es)")
            self.new_matches_detected = True
            return True
        else:
            print("✓ No new matches detected")
            self.new_matches_detected = False
            return False
        
    def update_most_recent_matches(self) -> None:
        """Update most_recent_matches.csv with new matches"""
        if not self.matches:
            print("No matches to save")
            return
        
        # Convert current matches to dataframe format
        current_matches_data = []
        for match in self.matches:
            for club_id, players in match.players_by_club.items():
                for player in players:
                    current_matches_data.append({
                        'matchId': match.match_id,
                        'timestamp': match.timestamp,
                        'timeAgo': match.time_ago,
                        'club_id': player.club_id,
                        'player_id': player.player_id,
                        'player_name': player.player_name,
                        'position': player.position,
                        'rating': player.rating,
                        'goals': player.goals,
                        'assists': player.assists,
                        'shots': player.shots,
                        'passes': player.passes,
                        'pass_attempts': player.pass_attempts,
                        'tackles': player.tackles,
                        'tackle_attempts': player.tackle_attempts,
                        'saves': player.saves,
                        'seconds_played': player.seconds_played,
                        'score': player.score,
                        'redcards': player.redcards,
                    })
        
        # Load existing most recent matches
        most_recent_df = self.load_most_recent_matches()
        
        if most_recent_df is not None:
            # Append new matches
            current_df = pd.DataFrame(current_matches_data)
            updated_df = pd.concat([most_recent_df, current_df], ignore_index=True)
            # Remove duplicates based on matchId and player_id
            updated_df = updated_df.drop_duplicates(subset=['matchId', 'player_id'], keep='last')
            updated_df.to_csv(self.most_recent_filepath, index=False)
            print(f"✓ Updated most_recent_matches.csv with new matches")
        else:
            # Create new most_recent_matches.csv
            df = pd.DataFrame(current_matches_data)
            df.to_csv(self.most_recent_filepath, index=False)
            print(f"✓ Created most_recent_matches.csv with {len(self.matches)} matches")
    
    def get_most_recent_match(self) -> Optional[MostRecentMatch]:
        """
        Get the most recent match as a MostRecentMatch object.
        Returns the match with is_new flag set based on new matches detection.
        """
        if not self.matches:
            return None
        
        # The first match in the list is typically the most recent
        most_recent_raw = self.matches[0]
        # Flatten all players from all clubs into a single list
        all_players = []
        for club_id, players in most_recent_raw.players_by_club.items():
            all_players.extend(players)
        
        self.most_recent_match = MostRecentMatch(
            match_id=most_recent_raw.match_id,
            timestamp=most_recent_raw.timestamp,
            time_ago=most_recent_raw.time_ago,
            players=all_players,
            is_new=self.new_matches_detected
        )
        
        return self.most_recent_match
    
    def export_most_recent_match_to_json(self, output_filepath: str) -> None:
        """Export only the most recent match to a JSON file"""
        if not self.matches:
            print("No matches to export")
            return
        
        # Get the most recent match (first in the list)
        most_recent = self.matches[0]
        
        match_data = {
            'match_id': most_recent.match_id,
            'timestamp': most_recent.timestamp,
            'time_ago': most_recent.time_ago,
            'clubs': {}
        }
        
        for club_id, players in most_recent.players_by_club.items():
            match_data['clubs'][club_id] = [
                {
                    'player_id': p.player_id,
                    'player_name': p.player_name,
                    'position': p.position,
                    'rating': p.rating,
                    'goals': p.goals,
                    'assists': p.assists,
                    'shots': p.shots,
                    'passes': p.passes,
                    'pass_attempts': p.pass_attempts,
                    'tackles': p.tackles,
                    'tackle_attempts': p.tackle_attempts,
                    'saves': p.saves,
                    'seconds_played': p.seconds_played,
                    'score': p.score,
                    'redcards': p.redcards,
                }
                for p in players
            ]
        
        # Wrap in a list to match the format of matches_readable.json
        data = [match_data]
        
        with open(output_filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✓ Exported most recent match (ID: {most_recent.match_id}) to {output_filepath}")
    
    def export_matches_to_json(self, output_filepath: str) -> None:
        """Export all parsed matches to a JSON file"""
        data = []
        for match in self.matches:
            match_data = {
                'match_id': match.match_id,
                'timestamp': match.timestamp,
                'time_ago': match.time_ago,
                'clubs': {}
            }
            
            for club_id, players in match.players_by_club.items():
                match_data['clubs'][club_id] = [
                    {
                        'player_id': p.player_id,
                        'player_name': p.player_name,
                        'position': p.position,
                        'rating': p.rating,
                        'goals': p.goals,
                        'assists': p.assists,
                        'shots': p.shots,
                        'passes': p.passes,
                        'pass_attempts': p.pass_attempts,
                        'tackles': p.tackles,
                        'tackle_attempts': p.tackle_attempts,
                        'saves': p.saves,
                        'seconds_played': p.seconds_played,
                        'score': p.score,
                        'redcards': p.redcards,
                    }
                    for p in players
                ]
            data.append(match_data)
        
        with open(output_filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✓ Exported {len(self.matches)} matches to {output_filepath}")
 
    def parse_csv(self) -> List[MatchData]:
        """Parse the CSV file and extract match data"""
        df = pd.read_csv(self.csv_filepath)
        
        for idx, row in df.iterrows():
            try:
                match = self._parse_match_row(row)
                self.matches.append(match)
            except Exception as e:
                print(f"Error parsing row {idx}: {e}")
                continue
        
        return self.matches

    def main(self):
        """Main entry point to parse matches"""
        matches = self.parse_csv()
        print(f"✓ Parsed {len(matches)} matches")
        has_new_matches = self.check_for_new_matches()
        #if has_new_matches:
        #    self.update_most_recent_matches()
        #    self.export_most_recent_match_to_json("newest_match.json")

        self.export_matches_to_json("matches_readable.json")

        return
    
class PlayerStatsParser:
    def __init__(self):
        self.json_file = "newest_match.json"

    def parse_stats(self):
        # Load the JSON file
        with open(self.json_file, 'r') as f:
            data = json.load(f)

        # Get the club data
        club_id = "3439844"
        players = data[0]['clubs'][club_id]

        # Define the stats to compare (excluding identifiers and position)
        stats_to_check = [
            'goals', 'assists', 'passes', 'tackles', 'redcards'
        ]

        # Create payloads for every player and every stat
        payloads = []

        for player in players:
            for stat in stats_to_check:
                # Create payload in format "PlayerName Stat Value"
                payload = f"{player['player_name']} {stat.replace('_', ' ').title()} {player[stat]}"
                payloads.append(payload)
                print(payload)

        return payloads


if __name__ == "__main__":
    MatchPlayerParser("club_matches.csv").main()