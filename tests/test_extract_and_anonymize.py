import pandas as pd
from scripts import extract_and_anonymize

# Test the atomic hashing function
def test_hash_string():
    name = "Abatangelo,Leonardo P"

    hash1 = extract_and_anonymize.hash_string(name)
    hash2 = extract_and_anonymize.hash_string(name)

    assert len(hash1) == 16
    assert hash1 == hash2  # Deterministic
    assert hash1 != extract_and_anonymize.hash_string("Someone,Else") # Different input, different hash

# Test the data cleaning logic
def test_handle_missing_names():
    # Setup dummy data
    data = {'Name': [" John Doe ", None]}
    df = pd.DataFrame(data)

    result_df = extract_and_anonymize.handle_missing_names(df)

    assert result_df['Name'][0] == "John Doe"  # Check strip()
    assert result_df['Name'][1] == "UNKNOWN_EMPLOYEE" # Check fillna()

# Test the full orchestration
def test_anonymize_data_removes_column():
    data = {'Name': ["Alice"], 'Salary': [50000]}
    df = pd.DataFrame(data)

    result_df = extract_and_anonymize.anonymize_data(df)

    assert 'Name' not in result_df.columns
    assert 'name_hash' in result_df.columns
    assert result_df['Salary'][0] == 50000

def test_unknown_employees_have_same_name_hash():
    """
    Checks if multiple unknown employees result in the same hashed.
    Note: Since hashing is deterministic, same input = same output.
    """
    data = {'Name': [None, None]}  # Two missing names
    df = pd.DataFrame(data)

    # 1. Cleaning (transforms None into UNKNOWN_EMPLOYEE)
    df_cleaned = extract_and_anonymize.handle_missing_names(df)

    # 2. Anonymization (hashes the "UNKNOWN_EMPLOYEE" string)
    result_df = extract_and_anonymize.anonymize_data(df_cleaned)

    # Verification
    # Since both names are "UNKNOWN_EMPLOYEE", they must result in the same name_hash
    assert result_df['name_hash'][0] == result_df['name_hash'][1]
