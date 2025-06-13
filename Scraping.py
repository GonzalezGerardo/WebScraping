import requests
from bs4 import BeautifulSoup
import polars as pl
import asyncio
import aiohttp
import json

# ========== Parte 1: Obtener productos ==========
def get_products(name_product, min_price, max_price):
    headers = {'User-Agent': 'Mozilla/5.0'}
    products = []

    for i in range(10):
        offset = f'_Desde_{i*50+1}' if i > 0 else ''
        url = (
            f'https://listado.mercadolibre.com.mx/{name_product.replace(" ", "-")}'
            f'{offset}_OrderId_PRICE_PriceRange_{min_price}-{max_price}_'
            'ITEM*CONDITION_2230284_NoIndex_True_SHIPPING*ORIGIN_10215068'
        )
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')

        for item in soup.select('.poly-card'):
            title_tag = item.select_one('.poly-component__title-wrapper')
            price_tag = item.select_one('.andes-money-amount__fraction')
            link_tag = item.select_one('.poly-component__title')

            if not title_tag or not price_tag or not link_tag:
                continue

            products.append({
                'Titulo': title_tag.text.strip(),
                'Precio': price_tag.text.strip(),
                'Enlace': link_tag['href']
            })
    return products

# ========== Parte 2: Filtro por título ==========
def titulo_valido(titulo):
    titulo = str(titulo).lower().strip()
    claves = ['polaina', 'pesa']
    prohibidas = ['grillete','escritorio', 'bebé', 'bebe', 'mascota', 'auto', 'carro', 
                  'niño', 'niña', 'comedor', 'bar', 'jardín', 'jardin']
    return any(titulo.startswith(k) for k in claves) and not any(p in titulo for p in prohibidas)

# ========== Parte 3: Scraping asíncrono ==========
async def extraer_detalles_async(session, row):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        async with session.get(row['Enlace'], headers=headers) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, 'html.parser')

            descripcion_tag = soup.find('p', class_='ui-pdp-description__content')
            descripcion = descripcion_tag.text.strip() if descripcion_tag else 'No disponible'

            claves_tags = soup.select('.andes-table__header')
            valores_tags = soup.select('.andes-table__column')
            caracteristicas = {
                claves_tags[i].get_text(strip=True): valores_tags[i].get_text(strip=True)
                for i in range(min(len(claves_tags), len(valores_tags)))
            }

            return {
                'Titulo': row['Titulo'],
                'Precio': row['Precio'],
                'Enlace': row['Enlace'],
                'Descripcion': descripcion,
                'Caracteristicas': caracteristicas
            }
    except Exception as e:
        print(f"Error al procesar {row['Enlace']}: {e}")
        return {
            'Titulo': row['Titulo'],
            'Precio': row['Precio'],
            'Enlace': row['Enlace'],
            'Descripcion': 'Error',
            'Caracteristicas': {}
        }

async def procesar_productos_async(filas):
    async with aiohttp.ClientSession() as session:
        tareas = [extraer_detalles_async(session, row) for row in filas]
        return await asyncio.gather(*tareas)

# ========== Parte 4: Script principal ==========
if __name__ == "__main__":
    product_name = 'pesas para tobillos'
    min_price = '80'
    max_price = '400'

    # Obtener productos y convertir a DataFrame
    productos = get_products(product_name, min_price, max_price)
    df = pl.DataFrame(productos)

    # Filtrar títulos válidos
    df_filtrado = df.filter(
        pl.col("Titulo").map_elements(titulo_valido, return_dtype=pl.Boolean)
    )

    # Guardar productos válidos a CSV
    df_filtrado.write_csv("productos.csv")

    # Ejecutar scraping asíncrono para detalles
    filas = list(df_filtrado.iter_rows(named=True))
    detalles = asyncio.run(procesar_productos_async(filas))

    # Guardar detalles en JSON
    with open('productos_detallados.json', 'w', encoding='utf-8') as f:
        json.dump(detalles, f, ensure_ascii=False, indent=4)

    print("✅ Scraping finalizado. Archivos guardados:")
    print(" - productos.csv")
    print(" - productos_detallados.json")
