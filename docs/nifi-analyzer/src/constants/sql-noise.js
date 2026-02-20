// Words to ignore when extracting SQL table references
export const SQL_NOISE = new Set([
  'select','where','set','and','or','as','on','in','is','not','null',
  'case','when','then','else','end','group','order','by','having',
  'limit','offset','union','all','exists','between','like','true',
  'false','values','into','for','if','with','from','table','now',
  'production','varchar','int','bigint','text','date','timestamp',
  'decimal','boolean','float','double','waiting','because','account',
  'log','Dates','LeadLag','startGrouping','grps','Temptation','dual'
]);
