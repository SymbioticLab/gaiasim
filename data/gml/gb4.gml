graph [
  label "gb4"
  node [
    id 1
    label "AS1"
  ]
  node [
    id 2
    label "AS2"
  ]
  node [
    id 3
    label "US1"
  ]
  node [
    id 4
    label "US2"
  ]
  node [
    id 5
    label "US3"
  ]
  node [
    id 6
    label "US4"
  ]
  node [
    id 7
    label "US5"
  ]
  node [
    id 8
    label "US6"
  ]
  node [
    id 9
    label "EU1"
  ]
  node [
    id 10
    label "EU2"
  ]
  node [
    id 11
    label "EU3"
  ]
  node [
    id 12
    label "EU4"
  ]
  edge [
    source 1
    target 2
    bandwidth 1000
  ]
  edge [
    source 1
    target 3
    bandwidth 6000
  ]
  edge [
    source 2
    target 5
    bandwidth 7000
  ]
  edge [
    source 3
    target 4
    bandwidth 2000
  ]
  edge [
    source 3
    target 6
    bandwidth 3000
  ]
  edge [
    source 4
    target 5
    bandwidth 1000
  ]
  edge [
    source 4
    target 7
    bandwidth 2000
  ]
  edge [
    source 4
    target 8
    bandwidth 2000
  ]
  edge [
    source 5
    target 6
    bandwidth 1000
  ]
  edge [
    source 6
    target 7
    bandwidth 1000
  ]
  edge [
    source 6
    target 8
    bandwidth 1000
  ]
  edge [
    source 7
    target 8
    bandwidth 1000
  ]
  edge [
    source 7
    target 11
    bandwidth 4000
  ]
  edge [
    source 8
    target 10
    bandwidth 500
  ]
  edge [
    source 9
    target 10
    bandwidth 1000
  ]
  edge [
    source 9
    target 11
    bandwidth 2000
  ]
  edge [
    source 10
    target 11
    bandwidth 1000
  ]
  edge [
    source 10
    target 12
    bandwidth 3000
  ]
  edge [
    source 11
    target 12
    bandwidth 3000
  ]
]
