# Stream In — source réseau SRT/RTMP

Ingère un flux réseau (contribution, OBS, encodeur distant…) et le publie sur le bus MXL
comme n'importe quelle source : câblable vers un multiview, un mélangeur, une sortie 2110…

## Modes d'ingest

- **SRT — écoute** : l'instance écoute sur son port ; la source distante pousse en mode
  *caller* vers l'URL affichée dans l'UI (`srt://<ip>:<port>?mode=caller`).
- **SRT — appel** : l'instance se connecte à une URL SRT distante (la source est en *listener*).
- **RTMP — serveur** : l'instance écoute en `rtmp://` ; une source type OBS pousse vers
  `rtmp://<ip>:<port>/<application>/<clé>`. Un seul publieur à la fois.
- **RTMP — pull** : l'instance tire un flux `rtmp://` existant.

L'IP affichée est celle du **conteneur** (macvlan) : elle est directement joignable sur le
LAN, sans redirection de port. Le port d'écoute est libre — vérifier qu'il ne collisionne
pas avec une autre instance sur le même nœud.

## Format de sortie (conform)

Le flux entrant est **toujours reconverti** au format configuré (résolution, cadence, chroma,
profondeur, balayage) : scale bicubique, conversion de cadence, désentrelacement automatique
si l'entrée est entrelacée. La sortie MXL est donc un signal broadcast propre et stable quel
que soit ce qu'envoie la source. Pour une sortie entrelacée (ex. 1080i50) : cadence = cadence
**trame** (25) et balayage = entrelacé.

## États

- **En attente de flux** : aucune image reçue — l'instance émet la mire (ou du noir, réglage
  « Image sans flux »).
- **Connecté** : images fraîches, sortie conformée en direct.
- **Gelé** : plus d'image depuis le seuil configuré — la dernière image est répétée (le signal
  MXL ne s'interrompt jamais). Reprise automatique dès que le flux revient ; en modes
  appel/pull, une connexion zombie est tuée et retentée automatiquement.

## Audio

L'audio du flux est décodé vers un shm 8 canaux 48 kHz (câblable page Câbles). Sans piste
audio dans le flux, l'instance bascule automatiquement en vidéo seule (silence sur le shm).

## Contrôle

- `GET /state` : état complet (connexion, fps entrée/sortie, âge de la dernière image,
  redémarrages ffmpeg, derniers messages d'erreur).
- `POST /reconnect` : force la coupure et la relance de la connexion (re-tente aussi l'audio).
