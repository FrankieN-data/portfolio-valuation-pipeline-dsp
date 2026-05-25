"""
Configuration tests for bronze layer ingestion pipeline.
"""
import sys
import os
import re

# Add transformations directory to Python path
transformations_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../transformations'))
sys.path.insert(0, transformations_dir)

# Import configuration variables (clean import, no DLT dependencies!)
from bronze_ingestion_config import VOLUME_ROOT, source_configuration_lst, source_lst


# =====================================================================
# CONFIGURATION TESTS
# =====================================================================

# 1. Test source configuration

def test_source_configuration_has_ten_entries():
    """Check we have exactly 10 table configurations."""
    assert len(source_configuration_lst) == 10, f"Expected 10 configs, got {len(source_configuration_lst)}"

def test_each_source_configuration_has_four_elements():
    """Each config tuple must have (filename, schema, source_system, comment)."""
    for i, config in enumerate(source_configuration_lst):
        assert len(config) == 4, f"Config {i} ({config[0] if len(config) > 0 else 'unknown'}) has {len(config)} elements, expected 4"

def test_all_expected_sources_are_configured():
    """Verify all 10 expected tables exist in source_configuration_lst (order doesn't matter)."""
    expected_tables = {
        "asset_quotations",
        "dim_asset", 
        "dim_company",
        "dim_wrapper",
        "equateplus_statement",
        "oneview_statement",
        "vanguard_isa_cash_statement",
        "vanguard_isa_transactions_statement",
        "vanguard_pension_cash_statement",
        "vanguard_pension_transactions_statement"
    }
    
    # Extract actual table names from configuration
    actual_tables = {config[0] for config in source_configuration_lst}
    
    # Check for missing or extra tables
    missing = expected_tables - actual_tables
    extra = actual_tables - expected_tables
    
    assert actual_tables == expected_tables, f"Table mismatch. Missing: {missing}, Extra: {extra}"


# 2. Test source file integration parameters

def test_source_lst_has_ten_entries():
    """Check we have exactly 10 source file integration list."""
    assert len(source_lst) == 10, f"Expected 10 source integration parameters, counted {len(source_lst)}"

def test_each_source_has_six_elements():
    """Each source tuple must have (path, source_system, schema, table_name, comment, primary_key_field)."""
    for i, source in enumerate(source_lst):
        assert len(source) == 6, f"Source {i} ({source[0] if len(source) > 0 else 'unknown'}) has {len(source)} elements, expected 6"

def test_dlt_assignments_match_source_lst():
    """Parse DLT file assignments and verify each assignment matches source_lst table names.
    
    For example, if the DLT file has:
        brz_asset_quotations = create_bronze_pipeline(*source_lst[0])
    
    Then source_lst[0][3] should equal "brz_asset_quotations".
    
    This validates that the actual DLT file assignments are correct, regardless of order.
    """
    dlt_file_path = os.path.join(transformations_dir, "01_bronze_csv_to_dlt.py")
    
    # Read the DLT file
    with open(dlt_file_path, 'r') as f:
        lines = f.readlines()
    
    # Pattern to match: brz_xxx = create_bronze_pipeline(*source_lst[N])
    pattern = r'^(\w+)\s*=\s*create_bronze_pipeline\(\*source_lst\[(\d+)\]\)'
    
    assignments_found = 0
    for line_num, line in enumerate(lines, 1):
        match = re.match(pattern, line.strip())
        if match:
            variable_name = match.group(1)  # e.g., "brz_asset_quotations"
            index = int(match.group(2))      # e.g., 0
            
            # Verify source_lst[index][3] matches variable_name
            actual_table_name = source_lst[index][3]
            
            assert actual_table_name == variable_name, \
                f"Line {line_num}: Assignment '{variable_name} = create_bronze_pipeline(*source_lst[{index}])' " \
                f"but source_lst[{index}][3] = '{actual_table_name}'. MISMATCH!"
            
            assignments_found += 1
    
    # Verify we found all 10 assignments
    assert assignments_found == 10, f"Expected 10 assignments in DLT file, found {assignments_found}"

def test_volume_root_format_is_valid():
    """VOLUME_ROOT should follow Unity Catalog volume path format."""
    assert VOLUME_ROOT.startswith("/Volumes/"), f"VOLUME_ROOT should start with /Volumes/, got: {VOLUME_ROOT}"
    assert "main" in VOLUME_ROOT, "VOLUME_ROOT should contain catalog name 'main'"
    assert "landing_files" in VOLUME_ROOT, "VOLUME_ROOT should contain volume name 'landing_files'"

def test_all_file_paths_use_volume_root():
    """All file paths in source_lst should use VOLUME_ROOT."""
    for i, config in enumerate(source_lst):
        file_path = config[0]
        assert file_path.startswith(VOLUME_ROOT), f"File path {i} doesn't use VOLUME_ROOT: {file_path}"

def test_all_file_paths_end_with_csv():
    """All file paths should end with .csv extension."""
    for i, config in enumerate(source_lst):
        file_path = config[0]
        assert file_path.endswith(".csv"), f"File path {i} doesn't end with .csv: {file_path}"

def test_all_filenames_are_unique():
    """No duplicate filenames - each table should read from a different file."""
    # Extract filenames from file paths
    filenames = []
    for config in source_lst:
        file_path = config[0]
        filename = file_path.split('/')[-1]  # Get last part of path
        filenames.append(filename)
    
    unique_filenames = set(filenames)
    assert len(filenames) == len(unique_filenames), f"Found duplicate filenames! {len(filenames)} files, {len(unique_filenames)} unique"

def test_all_table_names_start_with_brz():
    """All DLT table names should follow bronze layer naming convention (brz_*)."""
    for i, config in enumerate(source_lst):
        table_name = config[3]  # 4th element is table name
        assert table_name.startswith("brz_"), f"Table name {i} doesn't start with 'brz_': {table_name}"

def test_all_source_systems_are_valid():
    """Source systems should be one of: LOCAL, EQUATEPLUS, ONEVIEW, VANGUARD."""
    valid_systems = {"LOCAL", "EQUATEPLUS", "ONEVIEW", "VANGUARD"}
    
    for i, config in enumerate(source_lst):
        source_system = config[1]  # 2nd element is source_system
        assert source_system in valid_systems, f"Source system {i} is invalid: {source_system}. Must be one of {valid_systems}"

def test_all_primary_key_fields_are_not_empty():
    """All primary key fields should be non-empty strings."""
    for i, config in enumerate(source_lst):
        primary_key_field = config[5]  # 6th element is primary_key_field
        assert isinstance(primary_key_field, str), f"Primary key field {i} is not a string: {type(primary_key_field)}"
        assert len(primary_key_field) > 0, f"Primary key field {i} is empty"


# =====================================================================
# RUN ALL TESTS (when script is executed directly)
# =====================================================================

if __name__ == "__main__":
    print("Running configuration tests...")
    
    test_functions = [
        test_source_configuration_has_ten_entries,
        test_each_source_configuration_has_four_elements,
        test_all_expected_sources_are_configured,
        test_source_lst_has_ten_entries,
        test_each_source_has_six_elements,
        test_dlt_assignments_match_source_lst,
        test_volume_root_format_is_valid,
        test_all_file_paths_use_volume_root,
        test_all_file_paths_end_with_csv,
        test_all_filenames_are_unique,
        test_all_table_names_start_with_brz,
        test_all_source_systems_are_valid,
        test_all_primary_key_fields_are_not_empty
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_functions:
        try:
            test_func()
            print(f"✓ {test_func.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"✗ {test_func.__name__}: {e}")
            failed += 1
    
    print(f"\n{passed} passed, {failed} failed")
    
    if failed > 0:
        sys.exit(1)
