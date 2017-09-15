set N;
/* substrate nodes */

set F;
/* flows */

param b{u in N, v in N};
/* bandwidth of edge (u,v) */

param fs{i in F};
/* flow start points */

param fe{i in F};
/* flow end points */

var f{i in F, u in N, v in N} >= 0;
/* flow variable */


var FR{i in F};
/*FlowRate variable */


maximize A: sum{i in F} FR[i];
/* Max the Flow */

s.t. capcon{u in N, v in N}: sum{i in F} f[i,u,v] <= b[u,v];
/* capacity constraint */

s.t. demsat1{i in F}: sum{w in N} f[i, fs[i], w] - sum{w in N} f[i, w, fs[i]] = FR[i];
s.t. demsat2{i in F}: sum{w in N} f[i, fe[i], w] - sum{w in N} f[i, w, fe[i]] = -FR[i];
/* demand satisfaction */

s.t. flocon{i in F, u in N diff {fs[i], fe[i]}}: sum{w in N} f[i, u, w] - sum{w in N} f[i, w, u] = 0;
/* flow conservation */

end;
