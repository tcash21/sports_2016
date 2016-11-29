library(googlesheets)
library(dplyr)

setwd("/home/ec2-user/sports2016/NCF")

gs_auth(token = 'ttt.rds')

#ncf <- gs_title("NCF")
iris_ss <- gs_key('1W0fTkdFCUNRRdGjXIOOmjeAyJbUUH2o6k0d6JgDcBCQ', visibility = 'private')

iris_ss <- iris_ss %>% 
  gs_edit_cells(input = c("what", "is", "a", "sepal", "anyway?"),
                anchor = "A2", byrow = TRUE)

iris_ss <- iris_ss %>% 
  gs_add_row(input = c("sepals", "support", "the", "petals", "butts"))
