import requests
from bs4 import BeautifulSoup
import pandas as pd
def get_products(name_product, minimal_price_limit, maximal_price_limit):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

    products = []
    for repeticion in range(5):
        rango = 0
        if repeticion > 0:
            rango = str(repeticion*50+1)
            response = requests.get('https://listado.mercadolibre.com.mx/'+name_product.replace(' ', '-')+'_Desde_'+rango+'_OrderId_PRICE_PriceRange_'+minimal_price_limit+'-'+maximal_price_limit+'_ITEM*CONDITION_2230284_NoIndex_True_SHIPPING*ORIGIN_10215068', headers=headers)
        else:
            response = requests.get('https://listado.mercadolibre.com.mx/'+name_product.replace(' ', '-')+'_OrderId_PRICE_PriceRange_'+minimal_price_limit+'-'+maximal_price_limit+'_ITEM*CONDITION_2230284_NoIndex_True_SHIPPING*ORIGIN_10215068', headers=headers)
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        for item in soup.select('.poly-card'):
            title_tag = item.select_one('.poly-component__title-wrapper')
            price_tag = item.select_one('.andes-money-amount--cents-superscript').select_one('.andes-money-amount__fraction')
            link_tag = item.select_one('.poly-component__title')
            if not title_tag or not price_tag or not link_tag:
                continue

            title = title_tag.text.strip()
            price = price_tag.text.strip()
            link = link_tag['href']
            products.append({'title': title, 'price': price, 'link': link})
    return products
# Definir variables
product_name = 'silla'
minimal_price_limit = '800'
maximal_price_limit = '3500'
# Obtener productos
products = get_products(product_name, minimal_price_limit, maximal_price_limit)
# Convertir a DataFrame
products_df = pd.DataFrame(products)
products_df.to_csv('productos.csv', index=False)


# Cargar el archivo CSV
df = pd.read_csv('productos.csv')
palabras_clave_inicio = ['silla', 'gamer', 'ejecutiva']
palabras_prohibidas = ['playa', 'bebé', 'bebe', 'mascota', 'auto', 'carro', 'niño', 'niña', 'comedor', 'bar', 'jardín', 'jardin']

def titulo_valido(titulo):
    titulo = str(titulo).lower().strip()
    for palabra in palabras_clave_inicio:
        if titulo.startswith(palabra):
            if any(p in titulo for p in palabras_prohibidas):
                return False
            return True
    return False
# Aplicar el filtro
df_filtrado = df[df['title'].apply(titulo_valido)]
df_filtrado.to_csv('productos_filtrados_bien.csv', index=False)