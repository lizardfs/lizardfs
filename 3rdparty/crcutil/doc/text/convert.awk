BEGIN {
  first_line = 1;
}

{
  if (first_line) {
    printf("%20s", "");
  }
  for (i = 1; i <= NF; ++i) {
    if (i == 1 && !first_line) {
      printf("%20s", $i);
    } else {
      printf(" &%6s", $i);
    }
  }
  printf("    \\\\\n");
  first_line = 0;
}
