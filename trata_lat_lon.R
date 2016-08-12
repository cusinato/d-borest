options(stringsAsFactors=F,scipen=99)
# rm(list=ls());gc()
require(data.table)


events <- fread("events.csv",
                colClasses=c("character","character","character",
                             "numeric","numeric"))
setkeyv(events,c("device_id","event_id"))

x <- events[latitude!=0 & longitude!=0, .(latitude, longitude)]
rm(events); gc()
# library(ggmap)
# qmplot(longitude, latitude,
#        data = as.data.frame(clust1$centers),
#        size = I(3),
#        darken = .3) + theme(legend.position="none")


# ja rodei o clustering (via dbscan) e salvei:
load('checkpoint_scan1.RData')
x2 <- x[scan1$cluster == 0, ]
load('checkpoint_scan2.RData')
#centróides:
centroides1 <- x[scan1$cluster != 0, .(longitude = mean(longitude), latitude = mean(latitude)), scan1$cluster[scan1$cluster != 0]]
centroides2 <- x2[scan2$cluster != 0, .(longitude = mean(longitude), latitude = mean(latitude)), scan2$cluster[scan2$cluster != 0]]
centroides2$scan2 <- ifelse(centroides2$scan2 == 0, 0, max(centroides1$scan1) + centroides2$scan2)
names(centroides1) <- names(centroides2) <- c('scan', 'longitude', 'latitude')
centroides <- rbind(centroides1, centroides2)
rm(centroides1, centroides2, scan1, scan2); gc()

# library(ggmap)
# qmplot(longitude, latitude,
#        data = centroides,
#        size = I(3),
#        darken = .3) + theme(legend.position="none")
# Ficou bem maneiro!

## agora, capeta, calcula a distância mínima, maxima e media a que cada dispositivo móvel esteve desses pontos
# calcula distancias
library(geosphere)
i <- seq(nrow(centroides))
# vou particionar o problema pq nao tenho 74.6Gb de RAM, em 30x porque sou pobre haha
n_split <- 30
i <- split(i, rep(1:n_split, each = nrow(centroides) / n_split))
# j <- 1
# system.time(
#   distancias <- apply(centroides[i[[j]], .(longitude, latitude)], 1,
#                       function(centro) distGeo(x[, .(longitude, latitude)], centro))) #ok, funfa
n_nucleos <- 3
library(parallel)
cl <- makeCluster(n_nucleos); clusterExport(cl, c('x')); clusterEvalQ(cl, library(data.table))
for(j in 1:n_split) {
  distancias <- parApply(cl, centroides[i[[j]], .(longitude, latitude)], 1,
                         function(centro) geosphere::distGeo(x[, .(longitude, latitude)], centro))
  save(distancias, file = paste0('distancias_', i[[j]][1], 'a', tail(i[[j]], 1), '.RData'))
  rm(distancias); gc()
}
# medidas resumo das distancias (min, max, mean)
#       - estou com receio de usar desvio-padrão pq tenho a sensaçao de que ele tem muita variação temporal, dimensao ignorada nesta análise
#       - assimetria pode captar movimentos pendulares ou de ferias, por exemplo (mas tenho dados suficientes?)


# agrega resultados à tabela de treinamento fazendo um pre filtro (tipo medidas de associação com a var resposta) se nao couber na memória
#       - e como imputar valores? (tem NA pra caralho)
#         > se for usar metodologia baseada em particionamento/árvore posso atribuir -1, 0, média ou algo assim

