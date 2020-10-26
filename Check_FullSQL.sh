cd Proc
echo ' MS SQL'
echo ' x087'
ls *x087*FSQL*.csv | wc -l
echo ' x089'
ls *x089*FSQL*.csv | wc -l
echo ' x090'
ls *x090*FSQL*.csv | wc -l
echo ' x091'
ls *x091*FSQL*.csv | wc -l
echo ' x092'
ls *x092*FSQL*.csv | wc -l
echo ' Linux'
echo ' x088'
ls *x088*FSQL*.csv | wc -l
echo ' x095'
ls *x095*FSQL*.csv | wc -l
echo ' x100'
ls *x100*FSQL*.csv | wc -l
echo ' x104'
ls *x104*FSQL*.csv | wc -l
echo ' x110'
ls *x110*FSQL*.csv | wc -l
echo ' x112'
ls *x112*FSQL*.csv | wc -l
echo ' x113'
ls *x113*FSQL*.csv | wc -l
echo ' x119'
ls *x119*FSQL*.csv | wc -l
echo ' x121'
ls *x121*FSQL*.csv | wc -l
echo ' x124'
ls *x124*FSQL*.csv | wc -l
echo ' x114'
ls *x114*FSQL*.csv | wc -l

echo ' x106'
ls *x106*FSQL*.csv | wc -l
echo ' x107'
ls *x107*FSQL*.csv | wc -l
echo ' x108'
ls *x108*FSQL*.csv | wc -l

echo ' x118'
ls *x118*FSQL*.csv | wc -l
echo ' x120'
ls *x120*FSQL*.csv | wc -l
echo ' x123'
ls *x123*FSQL*.csv | wc -l
echo ' x034'
ls *x034*FSQL*.csv | wc -l
echo ' x035'
ls *x035*FSQL*.csv | wc -l
echo ' x150'
ls *x150*FSQL*.csv | wc -l

echo ' x101'
ls *x101*FSQL*.csv | wc -l
echo ' x102'
ls *x102*FSQL*.csv | wc -l
echo ' x103'
ls *x103*FSQL*.csv | wc -l
echo ' x109'
ls *x109*FSQL*.csv | wc -l
echo ' x151'
ls *x151*FSQL*.csv | wc -l
echo ' x105'
ls *x105*FSQL*.csv | wc -l
echo ' x106'
ls *x106*FSQL*.csv | wc -l

echo '---------------------------------------'
echo '             Prog File'
ls -lrt Full*Prog*
ls -lrt Full*Prog* | wc -l
echo ' '
echo '---------------------------------------'
echo '             Running Processes'
ps -ef | grep python3 | grep FullS
ps -ef | grep python3 | grep FullS | wc -l
echo ' '
echo '---------------------------------------'
echo '             Full SQL BAD there '
ls -l  *FSQL*.bad | wc -l
echo '---------------------------------------'
echo '             Full SQL csv there '
ls -l  *FSQL*.csv | wc -l
cd ..
echo '---------------------------------------'
echo '             Full SQL .gz there '
ls -l  *FSQL*.gz | wc -l
echo '---------------------------------------'

