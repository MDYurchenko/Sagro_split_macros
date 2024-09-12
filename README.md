### Пайплайн для преобразования статистики продаж по клиентам, регионам, группам продукции и конкретным продуктам в прогнозный спрос на следующий период.

Задача:
>Имеются данные по продаже продукции за несколько лет. Необходимо вывести статистический тренд продаж конкретной продукции по **месяцам**, по **региону**, по **клиенту**, по **группе товаров** и **конкретному товару**.
>Эти данные использовать для прогноза продаж продукции по тем же группам, учитывая, что предполагаемый годовой рост продаж составляет **N %**.
>
Входные данные:

Файлы с историей продаж за 5 лет, за год и за квартал.

Выходные данные: 

Файл с объемами продаж, где для каждого продукта в каждом регионе у конкретного клиента в каждый месяц известен прогнозируемый объем продаж.

Файловая структура проекта:
* все необходимые функции сосредоточены в пакете data_pipeline:
* 1. input_values.py:
     >Считывает основные зависимости из файла input_dependences.xlsx, который заполняет оператор.
  2. load_tabels.py:
     >загружает основные таблицы, указанные в input_dependences.xlsx.
  3. preprocessing_functions.py:
     >Выполняет инициализацию функций для преобразования загруженных данных. Функции заменяют старые ID продуктов на новые (исторически сложилось), исправляют ошибки в регионах и названиях продуктов.
  4. split_funcitons.py:
     >Выполняет инициализацию функций для вычисления средних долей по загруженным статистикам.
  5. volumes_functions.py:
     >Выполняет инициализацию функций для вычисления прогнозируемых объемов по расчитанным долям и полученному проценту роста N.
  6. month_3hier_split.py
     >Выполняет
  8. chanel_split.py
     >Выполняет
  10. region_split.py
      >Выполняет
  12. volumes_applying.py
      >Выполняет
  14. product_split.py
      >Выполняет
  16. template_volumes.py
      >Выполняет
  18. template_fractions.py
      >Выполняет
  20. export_data.py
      
