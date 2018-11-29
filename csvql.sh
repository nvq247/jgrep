tmpfile=$(mktemp)
errfile=$(mktemp)
set -f
echo "
{
  \"version\": \"1.0\",
  \"defaultSchema\": \"CSV\",
  \"schemas\": [
    {
      \"name\": \"CSV\",
      \"type\": \"custom\",
      \"factory\": \"org.apache.calcite.adapter.csv.CsvSchemaFactory\",
      \"operand\": {
        \"directory\": \"$(pwd -W)\"
      }
    }
  ]
}
" > $tmpfile
java  -cp "./csvql.jar" sqlline.SqlLine --verbose=true \
-u "jdbc:calcite:model=$tmpfile" \
--silent=true \
--showWarnings=false \
-e "$( if [[ "$1" ]]; then echo  $1  ; else echo '!tables' ; fi )"  \
2>$errfile ;cat $errfile |grep -Po "(Exception:.+)"