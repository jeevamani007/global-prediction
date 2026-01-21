from application_purpose_analyzer import ApplicationPurposeAnalyzer

def test_analyzer():
    analyzer = ApplicationPurposeAnalyzer()
    # Mock data
    line1 = "Test Purpose Line 1"
    line2 = "Test Connection Line 2"
    file_count = 2
    total_columns = 10
    has_relationships = True
    pattern_matches = {'account_management': 5}

    points = analyzer._generate_detailed_explanation(line1, line2, file_count, total_columns, has_relationships, pattern_matches)
    print("Generated Points:")
    for p in points:
        print(f"- {p}")

if __name__ == "__main__":
    test_analyzer()
