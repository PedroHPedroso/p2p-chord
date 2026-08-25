# Chord em Node.js

Implementação didática de um anel Chord com espaço fixo de identificadores `1..32`
(`m = 5`). Cada nó mantém cinco entradas na finger table e oferece entrada e
saída controladas por HTTP. Requer Node.js 18 ou superior e não usa pacotes
externos.

## Executar

Inicie o painel controlador, que utiliza a porta `5000`:

```bash
npm start
```

Abra `http://127.0.0.1:5000`. Na página, informe o ID, IP e porta do novo
nó. A porta `5000` fica reservada ao painel; utilize `5001`, `5002`, `5003` etc.

Para o primeiro nó, por exemplo:

- ID: `8`
- IP: `127.0.0.1`
- Porta de início: `5001`
- Opção: **Criar um novo anel**

Para o segundo nó, informe seu próprio ID/IP/porta (`20`, `127.0.0.1`, `5002`)
e selecione **Entrar por um nó existente**. Como destino, informe o nó 8 em
`127.0.0.1:5001`.

O painel controlador lista todos os nós locais em execução. Use **Abrir painel**
para ver o predecessor, sucessor, anel e finger table de um nó específico. Por
exemplo, o painel do nó na porta `5001` estará em
`http://127.0.0.1:5001`, e seu estado JSON em
`http://127.0.0.1:5001/api/state`.

Na mesma máquina, os nós compartilham o IP `127.0.0.1`, mas obrigatoriamente
usam portas diferentes.

### Executar em máquinas da rede local

Em cada máquina, execute `npm start` e abra o painel usando o IP da própria
máquina, por exemplo `http://172.16.1.10:5000`. O servidor escuta em todas as
interfaces e o painel preenche automaticamente o campo **IP desta máquina**.

Na primeira máquina, crie um novo anel. Nas demais, escolha **Entrar por um nó
existente** e informe o ID, IP e porta do primeiro nó (por exemplo,
`1`, `172.16.1.10`, `5001`). Cada nó deve anunciar o IP `172.16.X.X` da máquina
em que está executando, nunca `127.0.0.1` ou `0.0.0.0`.

Libere no firewall TCP a porta `5000` para o painel e as portas usadas pelos
nós (`5001`, `5002` etc.). Confirme a comunicação de outra máquina com:

```bash
curl http://172.16.1.10:5001/api/state
```

## Join

Ao receber `POST /join`, o nó:

1. cria sozinho um novo anel quando `bootstrap` não é informado; ou
2. pede ao nó de entrada o sucessor de seu próprio ID;
3. liga-se ao predecessor e ao sucessor encontrados;
4. calcula as cinco entradas para `n + 1`, `n + 2`, `n + 4`, `n + 8` e `n + 16`.
5. percorre o anel para atualizar as finger tables dos demais nós.

## Saída controlada

No painel controlador, use **Sair da rede** para retirar um nó local. Pela API,
envie `DELETE` para a porta do nó no controlador:

```bash
curl -X DELETE http://127.0.0.1:5000/api/nodes/5002
```

Antes de fechar o servidor, o nó:

1. transfere seus arquivos primários, inclusive `catalogo.txt`, ao sucessor;
2. faz o predecessor apontar para o sucessor e vice-versa;
3. percorre o anel e aguarda a atualização das finger tables;
4. sincroniza novamente os arquivos que possam ter mudado durante a saída;
5. fecha o servidor somente depois da conclusão dessas etapas.

Se a transferência ou a religação falhar, o nó permanece aberto e registrado
no painel. Ao encerrar `npm start` com `Ctrl+C`, o controlador tenta retirar os
nós locais sequencialmente antes de fechar.

Fechar uma aba do navegador não retira o nó da rede. Todos os nós criados por
um painel são servidores lógicos no mesmo processo Node.js; portanto, encerrar
esse processo à força encerra todos eles. Quedas abruptas de processo, máquina
ou conexão ainda não são recuperadas automaticamente: essa tolerância exige
estabilização periódica, detecção de falhas e uma lista de sucessores.

Execute os testes com:

```bash
npm test
```

## Arquivos: `put` e `get`

Cada `ChordNode` oferece `put(nome, conteúdo)` e `get(nome)`. O SHA-256 do nome
é convertido para uma posição entre 1 e 32; `findSuccessor` escolhe o primeiro
nó ativo nessa posição ou depois dela. Assim, posições sem nó são naturalmente
armazenadas no próximo nó ativo do anel.

```js
await node.put('trabalho.txt', Buffer.from('conteúdo'));
const arquivo = await node.get('trabalho.txt');
console.log(arquivo.content.toString());
```

Todo `put` também atualiza `catalogo.txt` (um nome por linha). O catálogo usa o
mesmo hash e é armazenado na própria rede. Pela API HTTP de qualquer nó:

O retorno do upload informa `primary`, `replicas` e `locations`. A gravação é
confirmada somente depois da tentativa de criar até duas réplicas nos sucessores
imediatos. A interface mostra o ID e o endereço de cada nó que confirmou uma
cópia. Para consultar novamente as localizações:

```bash
curl 'http://127.0.0.1:5001/api/files/locations?name=trabalho.txt'
```

Durante o download, se o nó primário não entregar o arquivo, o nó consultado
percorre as referências conhecidas e usa uma réplica disponível. Na saída
controlada, o primário também é promovido no sucessor antes de o servidor fechar.

```bash
curl -X POST http://127.0.0.1:5001/api/files \
  -H 'content-type: application/json' \
  -d '{"name":"trabalho.txt","content":"conteúdo"}'

curl -OJ 'http://127.0.0.1:5001/api/files?name=trabalho.txt'
curl 'http://127.0.0.1:5001/api/files?name=catalogo.txt'
```

Para bytes arbitrários, envie `content` em Base64 e acrescente
`"encoding":"base64"` ao JSON do `POST`.
