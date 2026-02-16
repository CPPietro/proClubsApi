import os
import shutil
from Match_Parser import MatchPlayerParser

def test_check_for_new_matches():
    """Test the check_for_new_matches function"""
    
    print("=" * 60)
    print("TEST 1: Testing with existing most_recent_matches.csv")
    print("=" * 60)
    
    # Create parser and load matches
    parser = MatchPlayerParser("club_matches.csv")
    matches = parser.parse_csv()
    print(f"\n✓ Loaded {len(matches)} matches from club_matches.csv")
    
    # Test check_for_new_matches with existing CSV
    print("\nCalling check_for_new_matches()...")
    result = parser.check_for_new_matches()
    print(f"Result: {result}")
    print(f"new_matches_detected flag: {parser.new_matches_detected}")
    
    # Show some match IDs
    print(f"\nFirst 3 match IDs loaded:")
    for match in matches[:3]:
        print(f"  - {match.match_id}")
    
    print("\n" + "=" * 60)
    print("TEST 2: Testing with NO most_recent_matches.csv")
    print("=" * 60)
    
    # Backup the existing CSV
    backup_path = "most_recent_matches.csv.backup"
    if os.path.exists("most_recent_matches.csv"):
        shutil.move("most_recent_matches.csv", backup_path)
        print(f"\n✓ Moved most_recent_matches.csv to backup")
    
    # Create new parser and test
    parser2 = MatchPlayerParser("club_matches.csv")
    matches2 = parser2.parse_csv()
    print(f"✓ Loaded {len(matches2)} matches from club_matches.csv")
    
    print("\nCalling check_for_new_matches() with no existing CSV...")
    result2 = parser2.check_for_new_matches()
    print(f"Result: {result2}")
    print(f"new_matches_detected flag: {parser2.new_matches_detected}")
    
    # Restore the backup
    if os.path.exists(backup_path):
        shutil.move(backup_path, "most_recent_matches.csv")
        print(f"\n✓ Restored most_recent_matches.csv from backup")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Test 1 Result: {'PASS' if isinstance(result, bool) else 'FAIL'}")
    print(f"Test 2 Result: {'PASS - Returns True when CSV missing' if result2 == True else 'FAIL'}")
    print("\n✓ Function verification complete!")

if __name__ == "__main__":
    test_check_for_new_matches()
