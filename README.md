![alt text](docs/images/priam.png "Priam Logo")

## Documentation Updates

To update the documentation, follow below steps

Documentation helper commands are in tox, so ensure you have tox installed on your machine

```
pip install tox
```
Change the documentation inside `docs/` folder. 

*To build:*

```
tox -e build
```

*To test the changes locally:*
```
tox -e test
```

*To deploy the changes to `gh-pages` branch:*
```
tox -e publish
```
