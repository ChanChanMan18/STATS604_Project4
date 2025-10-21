current_date <- format(Sys.Date(), "%Y-%m-%d")
vector_of_zeros <- rep(0, 754)
output_vec <- c(current_date, vector_of_zeros)
formatted_output <- paste(output_vec, 
                          collapse = ", ")

cat(sprintf("\"%s\"", formatted_output))
