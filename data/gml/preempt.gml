graph [
    label "preempt"
    node [
        id          0
        label       "n1"
        longitude   1
        Latitude    2
    ]
    node [
        id          1
        label       "n2"
        longitude   6
        Latitude    1
    ]
    node [
        id          2
        label       "n3"
        longitude   3
        Latitude    3
    ]
    node [
        id          3
        label       "n4"
        longitude   8
        Latitude    8
    ]
    edge [
        source      0
        target      1
        bandwidth   2
    ]
    edge [
        source      2
        target      1
        bandwidth   2
    ]
    edge [
        source      1
        target      3
        bandwidth   2
    ]
]
