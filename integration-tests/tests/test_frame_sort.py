from setup import tc

data = [[1,0,"a"],[2,1,"b"],[2,1,"c"],[1,1,"d"],[2,2,"e"]]

def test_frame_sort_mixed_directions(tc):
    """
    Tests sorting a frame with different ascending/descending directions specified for each column, using a
    list of tuples.  Tests with backend rdd as scala, then as python (both should work the same way).
    """

    # Tuples specifying column sort directions
    sort_directions = [('C0', True),('C1',False),('C2',True)]

    # Expected data after the sort
    expected_sort = [[1, 1, 'd'],[1,0,'a'],[2, 2, 'e'],[2,1,'b'],[2,1,'c']]

    # Create frame, and sort when backend rdd is scala
    frame = tc.frame.create(data)
    frame._scala
    assert(frame._is_scala)
    frame.sort(sort_directions)
    assert(frame.take(frame.count()) == expected_sort)

    # Create frame, and sort when backend rdd is python
    frame = tc.frame.create(data)
    frame._python
    assert(frame._is_python)
    frame.sort(sort_directions)
    assert(frame.take(frame.count()) == expected_sort)

def test_frame_sort_same_direction(tc):
    """
    Tests sorting a frame with each column specifying the same sort direction.  Tests with backend rdd as scala, then as python (both should work the same way).
    """

    # Tuples specifying column sort directions
    sort_directions = [('C0', True),('C2',True)]

    # Expected data after the sort on C0, C2
    expected_sort = [[1, 0, 'a'],[1,1,'d'],[2, 1, 'b'],[2,1,'c'],[2,2,'e']]

    # Create frame, and sort when backend rdd is scala
    frame = tc.frame.create(data)
    frame._scala
    assert(frame._is_scala)
    frame.sort(sort_directions)
    assert(frame.take(frame.count()) == expected_sort)

    # Do the same as above, except specify column names as a list instead of list of tuples
    frame = tc.frame.create(data)
    frame._scala
    assert(frame._is_scala)
    frame.sort(["C0", "C2"], True)
    assert(frame.take(frame.count()) == expected_sort)

    # Create frame, and sort when backend rdd is python
    frame = tc.frame.create(data)
    frame._python
    assert(frame._is_python)
    frame.sort(sort_directions)
    assert(frame.take(frame.count()) == expected_sort)

    # Do the same as above, except specify column names as a list instead of list of tuples
    frame = tc.frame.create(data)
    frame._python
    assert(frame._is_python)
    frame.sort(["C0", "C2"], True)
    assert(frame.take(frame.count()) == expected_sort)