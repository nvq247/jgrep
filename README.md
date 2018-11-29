# install jgrep
wget https://raw.githubusercontent.com/nvq247/jgrep/master/install-jgrep.sh -O install-jgrep.sh && bash install-jgrep.sh

curl https://raw.githubusercontent.com/nvq247/jgrep/master/install-jgrep.sh -o install-jgrep.sh && bash install-jgrep.sh

#expamle 1
cat file|jgrep '(\d+)(\w+)' '$2  $1'
#expamle 2
cat file|jgrep "(\\d+)(\\w+)" "\$2"



====================== pipe.jar ===============================

Redirect out put to multi stream (file or url or stdout..)

x: exit when error

i: read from stdin

o: print stdout

e: print to stder

b: output buffer (byte)

B: input buffer (byte)

t: url raw data write

s: show percent

f: overwrite file if exists

a: append when file exists

java -jar pipe.jar infile outfile1 outfile2 outfile3

---or---
alias pipe='java -jar path_to_pipe.jar'

pipe infile outfile1 outfile2 outfile3

pipe infile url url url

cat 123|pipe -i    outfile0 outfile1 outfile2 outfile3

cat 123|pipe -i -s outfile0 outfile1 outfile2 outfile3

cat 123|pipe -i    sock::999 outfile1 outfile2 outfile3

cat 123|pipe -i    infile0 sock:outhost:999 outfile2 outfile3



=================================== CSVQL =======================================

java8 require!!

csvql "select \"column\" from \"file_name_csv\" where "colunm2" ='1' ";

csvql "!table"
