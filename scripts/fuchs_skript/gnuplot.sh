#!/bin/sh

# JULEA - Flexible storage framework
# Copyright (C) 2013 Anna Fuchs
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

set -ex

rc=0

ROOT="$1"

cd $ROOT
#hostname must not have any "_"
host=$(echo "$PWD" | sed -e 's/^.*_//')
month=$(ls -t1 | head -n 1)
cd $month

rm -rf pdf || rc=$?
rm -rf toplot || rc=$?

#directories by last change
list=$(ls -tr1)
size=$(ls -tr1 | wc -l)

mkdir toplot

for result in $list
do
  cd $result

  case_groups=$(ls -1)
  for group in $case_groups
  do
    number=$(wc -l < $group)

    i=2
    while [ $i -le $number ]
    do
      tcase=$(head -n $i $group | tail -n 1 | cut -d' ' -f1 | sed -e 's#^/##' -e 's#/#_#g')

      echo -n "$result " | sed -e 's#^....-..-##'>> $ROOT/$month/toplot/${group}_${tcase}.toplot
      head -n $i $group | tail -n 1 >> $ROOT/$month/toplot/${group}_${tcase}.toplot

      i=$(expr $i + 1)
    done
  done
  cd ..
done

mkdir -p pdf

cd toplot
plots=$(ls -1)

for toplot in $plots
do

  number=0
  number=$(cat $ROOT/$month/toplot/$toplot | wc -l)

  case=$(tail -n +1 $ROOT/$month/toplot/$toplot | head -n 1| cut -d' ' -f2 | sed -e 's#^/##' -e 's#_#/#g')
  thput=$(tail -n +1 $ROOT/$month/toplot/$toplot | head -n 1| cut -d' ' -f5)
  col=0

  if [ "$thput" = "-" ]
  then
    col=4
  else
    col=5
  fi

  tit=$(echo "$host : $month, $case")

  gnuplot << EOF

  plot "$ROOT/$month/toplot/$toplot" using 3:xticlabels(1)    #To get the max and min value
  loc_time_min=GPVAL_DATA_Y_MIN
  loc_time_max=GPVAL_DATA_Y_MAX

  plot "$ROOT/$month/toplot/$toplot" using $col:xticlabels(1)    #To get the max and min value
  thr_min=GPVAL_DATA_Y_MIN
  thr_max=GPVAL_DATA_Y_MAX

  plot "$ROOT/$month/toplot/$toplot" using 6:xticlabels(1)    #To get the max and min value
  glob_time_max=GPVAL_DATA_Y_MAX

  set terminal pdfcairo enhanced font "Palatino,9" fontscale 0.6 linewidth 2 rounded size 5+($number)/2cm,7.5cm
  set output "$ROOT/$month/pdf/${toplot%.toplot}.pdf"

  # Tango palette
  set linetype 1 linecolor rgb "#ef2929"
  set linetype 2 linecolor rgb "#8ae234"
  set linetype 3 linecolor rgb "#729fcf"
  set linetype 4 linecolor rgb "#ad7fa8"
  set linetype 5 linecolor rgb "#fcaf3e"
  set linetype 6 linecolor rgb "#888a85"
  set linetype 7 linecolor rgb "#fce94f"
  set linetype 8 linecolor rgb "#e9b96e"
  set linetype cycle 8

  set title "$tit"
  set xlabel "commit [date - hash]"

  set ylabel "runtime [sec]" textcolor rgb "#ef2929" font "Palatino,10" #rotate by 270
  set y2label ($col==4  ? "throughput in [ops/sec]" : "throughput in [MB/sec]") textcolor rgb "#729fcf" font "Palatino,10"

  set key outside below samplen 2

  y_from=((loc_time_min-3)<0 ? 0 : (loc_time_min-3))
  y_to=(glob_time_max+3)
  set yrange [y_from:y_to]

  y2_from=($col==5 ? (((thr_min-(thr_min*0.15)))/1024.0/1024.0) : (thr_min-(thr_min*0.15)) )
  y2_to=($col==5 ? (((thr_max+(thr_max*0.15)))/1024.0/1024.0) : (thr_max+(thr_max*0.15)) )
  set y2range [y2_from : y2_to ]

  set mytics 5

  set format y "%g"
  set format y2 "%g"

  set xtics nomirror rotate by -45
  set ytics nomirror
  set y2tics nomirror

  set style fill solid border -1

  set border 11

  set datafile missing "-"
  test=$col

  set label 1 gprintf("min %g", loc_time_min) at 0, ((loc_time_min-1)<0 ? 0.2 : (loc_time_min-1)) tc rgb "#ef2929" enhanced font "Courier New,7"
  set label 2 gprintf("max %g", thr_max) at 0, glob_time_max+2.5 tc rgb "#729fcf" enhanced font "Courier New,7"

  plot '$ROOT/$month/toplot/$toplot' using 3:xticlabels(1) title "test time" with linespoints linewidth 2 linetype 1 pt 7 ps 0.2 axes x1y1, \
                              '' using 6          title "total time" with lines linewidth 1 linetype 1 axes x1y1, \
                              loc_time_min    title "min. time" with lines linewidth 2 linetype "dashed" linecolor rgb "#ef2929" axes x1y1, \
                              '' using ($col==4 ? \$4 : (\$5 / 1024 /1024))       title "throughput" with linespoints linewidth 2 linetype 3 pt 5 ps 0.2 axes x2y2, \
                             ($col==4 ? thr_max : (thr_max / 1024 /1024))          title "max. throughput" with lines linewidth 3 linetype "dashed" linecolor rgb "#729fcf" axes x2y2


  EOF

done

exit 0
