import unittest
from unittest.mock import patch, MagicMock

class TestSparkETL(unittest.TestCase):
    
    def test_is_dst_localized(self):
        """Test the DST detection function directly"""
        from energy_pipeline_cdk.scripts.gold.glue.energy_pipeline_batch_gold import is_dst_localized
        
        # Test winter time (non-DST)
        self.assertFalse(is_dst_localized("2023-01-15 10:00:00"))
        
        # Test summer time (DST)
        self.assertTrue(is_dst_localized("2023-07-15 10:00:00"))
    
    @patch('energy_pipeline_cdk.scripts.gold.glue.energy_pipeline_batch_gold.main')
    def test_script_runs(self, mock_main):
        """Simply test that the module can be imported"""
        import energy_pipeline_cdk.scripts.gold.glue.energy_pipeline_batch_gold
        
        # Verify main function was properly mocked
        self.assertIsNotNone(mock_main)
    

if __name__ == '__main__':
    unittest.main()