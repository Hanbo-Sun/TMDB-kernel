#setwd("project_parta_report_hanbosun/data/")
require(ggplot2)
#visualization the distribution of the release year
year = read.table("year.txt",header = F)
png("release_year_hist.png",width=1200,height=800)
ggplot(data=year, aes(year$V1)) + 
  geom_histogram(binwidth=1) + 
  scale_x_continuous(breaks=1916:2017) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+
  ggtitle("Release Years, total 4843 films")
dev.off()


t = read.csv("year_rate_duration_budge.csv")
df1 = t[1:89,1:2]
df2 = t[1:89,c(1,3)]
df3 = t[1:89,c(1,4)] # ignore 2017 (since only one movie in 2017 recorded)
png("rate_year.png",width=1200,height=800)
ggplot(data=df1, aes(Year,Average.Rate)) + 
  geom_line()+
  scale_x_continuous(breaks=1916:2016) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+
  ggtitle("Average Rate v.s. time")
dev.off()

png("duration_year.png",width=1200,height=800)
ggplot(data=df2, aes(Year,Average.Duration)) + 
  geom_line()+
  scale_x_continuous(breaks=1916:2016) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+
  ggtitle("Average Duration v.s. time")
dev.off()

df3 = df3[df3$ave_budget<50000000,] # control one outlier
png("budget_year.png",width=1200,height=800)
ggplot(data=df3, aes(Year,ave_budget)) + 
  geom_line()+
  scale_x_continuous(breaks=1916:2016) + 
  theme(axis.text.x = element_text(angle = 90, hjust = 1))+
  ggtitle("Average Budget v.s. time")
dev.off()

