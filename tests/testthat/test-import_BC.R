library(MySQLimport)

data = import_BC(
  system.file("extdata", "bc.txt", package="MySQLimport"),
  MySQL = FALSE)

test_that("BC lido corretamente", {
  expect_equal(
    data$nome,
    "ALBERSON DA SILVA MIRANDA")
})
