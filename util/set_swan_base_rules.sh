curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:01", "name":"flow-mod-1","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.1", "eth_type":"0x0800", "active":"true", "actions":"output=1" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:01", "name":"flow-mod-2","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.2", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:01", "name":"flow-mod-3","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.3", "eth_type":"0x0800", "active":"true", "actions":"output=3" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:01", "name":"flow-mod-4","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.4", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:01", "name":"flow-mod-5","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.5", "eth_type":"0x0800", "active":"true", "actions":"output=3" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:02", "name":"flow-mod-6","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.1", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:02", "name":"flow-mod-7","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.2", "eth_type":"0x0800", "active":"true", "actions":"output=1" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:02", "name":"flow-mod-8","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.3", "eth_type":"0x0800", "active":"true", "actions":"output=3" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:02", "name":"flow-mod-9","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.4", "eth_type":"0x0800", "active":"true", "actions":"output=4" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:02", "name":"flow-mod-10","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.5", "eth_type":"0x0800", "active":"true", "actions":"output=3" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:03", "name":"flow-mod-11","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.1", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:03", "name":"flow-mod-12","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.2", "eth_type":"0x0800", "active":"true", "actions":"output=3" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:03", "name":"flow-mod-13","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.3", "eth_type":"0x0800", "active":"true", "actions":"output=1" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:03", "name":"flow-mod-14","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.4", "eth_type":"0x0800", "active":"true", "actions":"output=4" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:03", "name":"flow-mod-15","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.5", "eth_type":"0x0800", "active":"true", "actions":"output=5" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:04", "name":"flow-mod-16","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.1", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:04", "name":"flow-mod-17","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.2", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:04", "name":"flow-mod-18","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.3", "eth_type":"0x0800", "active":"true", "actions":"output=3" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:04", "name":"flow-mod-19","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.4", "eth_type":"0x0800", "active":"true", "actions":"output=1" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:04", "name":"flow-mod-20","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.5", "eth_type":"0x0800", "active":"true", "actions":"output=4" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:05", "name":"flow-mod-21","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.1", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:05", "name":"flow-mod-22","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.2", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:05", "name":"flow-mod-23","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.3", "eth_type":"0x0800", "active":"true", "actions":"output=2" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:05", "name":"flow-mod-24","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.4", "eth_type":"0x0800", "active":"true", "actions":"output=3" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
curl -X POST -d  '{"switch":"00:00:00:00:00:00:00:05", "name":"flow-mod-25","cookie":"0", "priority":"50","ipv4_dst":"10.0.0.5", "eth_type":"0x0800", "active":"true", "actions":"output=1" }' http://127.0.0.1:8080/wm/staticentrypusher/json ;
