#!/usr/bin/env python
# coding: utf-8

# In[1]:


from setuptools import setup, find_packages


# In[ ]:


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='data_digger',
    version='23.12.10',
    description="Python beginner's project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author = "Yana Karausheva",
    author_email="yana-karausheva@yandex.ru",
    install_requires = [
        'aiohttp>=3.6.2',
        'beautifulsoup4>=4.8.2',
        'dnspython>=2.0.0',
        'nltk==3.5',
        'numpy>=1.17.2',
        'pymongo>=3.11.1',
        'pytz>=2019.2',
        'Scrapy >= 2.3.0',
        'Twisted==20.3.0'
    ],
    packages=find_packages('data_digger', 'data_digger.stack'),
    scripts = ['data_digger/main.py'],
    python_requires='>=3.6',
)

