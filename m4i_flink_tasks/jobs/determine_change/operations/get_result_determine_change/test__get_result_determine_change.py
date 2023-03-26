from typing import List

from .get_result_determine_change import GetResultDetermineChange


def test_flat_map_with_empty_list():
    get_result = GetResultDetermineChange()
    input_list: List[str] = []
    output_list = list(get_result.flat_map(input_list))
    assert output_list == []


def test_flat_map_with_single_element_list():
    get_result = GetResultDetermineChange()
    input_list = ['a']
    output_list = list(get_result.flat_map(input_list))
    assert output_list == ['a']


def test_flat_map_with_multiple_elements_list():
    get_result = GetResultDetermineChange()
    input_list = ['a', 'b', 'c']
    output_list = list(get_result.flat_map(input_list))
    assert output_list == ['a', 'b', 'c']
