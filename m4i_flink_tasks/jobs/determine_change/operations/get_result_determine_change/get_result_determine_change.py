from typing import Generator, List

from pyflink.datastream.functions import FlatMapFunction


class GetResultDetermineChange(FlatMapFunction):
    """
    A class to implement the FlatMapFunction interface, which transforms input data
    by applying a flat_map method. In this case, the flat_map method simply yields
    each element of the input_list without making any changes.
    """

    def flat_map(self, value: List[str]) -> Generator[str, None, None]:
        """
        The flat_map method takes a list of strings as input and yields each element
        without making any changes. This method provides an identity operation, which
        can be useful when you want to use a flat_map function but not change the input.

        Args:
        value (List[str]): A list of input strings to be passed through the flat_map method.

        Yields:
        element (str): Each element of the input list as is, without any transformation.
        """

        for element in value:
            yield element
        # END LOOP
    # END flat_map
# END GetResultDetermineChange
