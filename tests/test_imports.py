import pytest

def test_importvicrunner():
    with pytest.raises(ImportError):
        from rat.core.run_vic import VICRunner