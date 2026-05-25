let stock = 5;

function getStock() {
  return stock;
}

function reduceStock(qty) {
  stock -= qty;
}

module.exports = { getStock, reduceStock };