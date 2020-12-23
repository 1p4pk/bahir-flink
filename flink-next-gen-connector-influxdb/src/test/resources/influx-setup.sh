influx setup \
       -f \
       -u any \
       -p 12345678 \
       -b Flight-Database \
       -o HPI \
       -r 0

influx write \
  -b Flight-Database \
  -o HPI \
  -p ns \
  -f /bird-migration.txt