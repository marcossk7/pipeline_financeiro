from typing import Self
import polars as pl
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger(__name__)

class PreProcessing:
    def __init__(self, dataframe:pl.DataFrame):
        self.df = dataframe
    
    def rename_columns(self) -> Self:
        logger.info('Renomear códigos!')
        self.df.columns = [
            'date',
            'close',
            'high',
            'low',
            'open',
            'volume'
        ]
        return self
    
    def change_dtypes(self) -> Self:
        logger.info('Mudança da coluna date para tipo Date')
        self.df = self.df.with_columns(
            pl.col('date').cast(pl.Date)
        )

        for col in self.df.columns:
            if col == 'volume':
                self.df = self.df.with_columns(
                    pl.col(col).cast(pl.Int64)
                )
            elif col != 'date':
                self.df = self.df.with_columns(
                    pl.col(col).cast(pl.Decimal(6,2))
                )
        return self
    
    def get_dataframe(self) -> pl.DataFrame:
        return self.df

def transform(dataframe:pl.DataFrame):
    logger.info('Instanciar dataframe para pré-processamento')
    ppr = PreProcessing(dataframe)
    
    df_tratado = (
        ppr
        .rename_columns()
        .change_dtypes()
        .get_dataframe()
    )

    return df_tratado
#%%

# %%
