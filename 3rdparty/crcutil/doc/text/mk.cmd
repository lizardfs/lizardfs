@echo off
pdflatex crc.tex
if not exist crc.bbl bibtex crc && pdflatex crc.tex && pdflatex crc.tex
start crc.pdf
