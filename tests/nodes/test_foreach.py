import pytest

from pargo import Foreach
from pargo.utils import add_item, add_y, double, get_items, triple


def test_foreach_with_function():
    """Test that Foreach run as expected gived a function"""
    data = {"x": 2}

    node = Foreach(get_items).then(double)
    result = node.run(data)

    assert "x" in result
    assert isinstance(result["x"], int)
    assert result["x"] == 4


def test_foreach_with_list(tmp_path):
    """Test that Foreach runs as expected given a list"""
    data = {"x": 5}

    node = Foreach({"item":["a", "b"]}).then(double)
    result = node.run(data)

    assert "x" in result
    assert isinstance(result["x"], int)
    assert result["x"] == 10


def test_foreach_with_item(tmp_path):
    """Test that Foreach runs as expected when using the items in the given list."""
    data = {"x": 5}

    node = Foreach({"item":[1, 2]}).then(add_item)
    result = node.run(data)

    assert "y" in result
    assert isinstance(result["y"], list)
    assert sorted(result["y"]) == [6, 7]


def test_foreach_with_named_item(tmp_path):
    """Test that Foreach runs as expected when using named items in the given list."""
    data = {"x": 1}

    node = Foreach({"y":[2, 5]}).then(add_y)
    result = node.run(data)

    assert "y" in result
    assert isinstance(result["y"], list)
    assert sorted(result["y"]) == [3, 6]
    assert result["x"] == 1


def test_foreach_with_empty_item(tmp_path):
    """Test that Foreach skips then for empty list."""
    data = {"x": 5}

    node = Foreach({"item":[]}).then(add_item)
    result = node.run(data)

    assert "y" not in result
    assert result == {"x": 5}


def test_foreach_get_templates_function():
    """Test that Foreach.get_templates produces the expected structure given a function."""
    node = Foreach(get_items).then(double)
    templates = node.get_templates(
        step_counter=1,
        default_image="image",
        image_pull_policy="Always",
        default_secrets=None,
        default_parameters=[],
        default_retry=2,
    )

    assert templates[0].name == "step-1-foreach-0"
    assert templates[1].name == "step-1-get-items"
    assert templates[2].name == "step-1-double"
    assert templates[3].name == "step-1-merge-0"


def test_foreach_get_templates_list():
    """Test that Foreach.get_templates produces the expected structure given a list."""
    node = Foreach({"item":["a", "b"]}).then(double)
    templates = node.get_templates(
        step_counter=1,
        default_image="image",
        image_pull_policy="Always",
        default_secrets=None,
        default_parameters=[],
        default_retry=None,
    )

    assert templates[0].name == "step-1-foreach-0"
    assert templates[1].name == "step-1-double"
    assert templates[2].name == "step-1-merge-0"


def test_foreach_then_twice_raises():
    """Test that wrong order fails."""
    node = Foreach(get_items).then(double)
    with pytest.raises(RuntimeError, match="must follow Foreach"):
        node.then(triple)


# def test_foreach_nested_run():
#     data = {"x": 5}

#     node = Foreach("item":["a", "b"]).then(Foreach(["a", "b"]).then(double))
#     result = node.run(data)

#     assert "x" in result
#     assert isinstance(result["x"], int)
#     assert result["x"] == 10
#     pass
