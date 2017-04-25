graph [
    label "paper"
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
        longitude   4
        Latitude    2
    ]
    node [
        id          3
        label       "n4"
        longitude   3
        Latitude    5
    ]
    edge [
        source      0
        target      2
        bandwidth   12
    ]
    edge [
        source      0
        target      3
        bandwidth   12
    ]
    edge [
        source      1
        target      2
        bandwidth   8
    ]
    edge [
        source      1
        target      3
        bandwidth   8
    ]
]
