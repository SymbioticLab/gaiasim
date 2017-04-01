graph [
    label "paper"
    node [
        id          1
        label       "HK"
        Longitude   1
        Latitude    1
    ]
    node [
        id          2
        label       "LA"
        Longitude   1
        Latitude    1
    ]
    node [
        id          3
        label       "NY"
        Longitude   1
        Latitude    1
    ]
    node [
        id          4
        label       "FL"
        Longitude   1
        Latitude    1
    ]
    node [
        id          5
        label       "BA"
        Longitude   1
        Latitude    1
    ]
    edge [
        source      1
        target      2
        bandwidth   5120
    ]
    edge [
        source      1
        target      3
        bandwidth   5120
    ]
    edge [
        source      2
        target      3
        bandwidth   5120
    ]
    edge [
        source      2
        target      4
        bandwidth   5120
    ]
    edge [
        source      3
        target      4
        bandwidth   5120
    ]
    edge [
        source      3
        target      5
        bandwidth   5120
    ]
    edge [
        source      4
        target      5
        bandwidth   5120
    ]
]
