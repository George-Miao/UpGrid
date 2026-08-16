(function(){const e=document.createElement("link").relList;if(e&&e.supports&&e.supports("modulepreload"))return;for(const a of document.querySelectorAll('link[rel="modulepreload"]'))s(a);new MutationObserver(a=>{for(const n of a)if(n.type==="childList")for(const o of n.addedNodes)o.tagName==="LINK"&&o.rel==="modulepreload"&&s(o)}).observe(document,{childList:!0,subtree:!0});function i(a){const n={};return a.integrity&&(n.integrity=a.integrity),a.referrerPolicy&&(n.referrerPolicy=a.referrerPolicy),a.crossOrigin==="use-credentials"?n.credentials="include":a.crossOrigin==="anonymous"?n.credentials="omit":n.credentials="same-origin",n}function s(a){if(a.ep)return;a.ep=!0;const n=i(a);fetch(a.href,n)}})();const ce=globalThis,qe=ce.ShadowRoot&&(ce.ShadyCSS===void 0||ce.ShadyCSS.nativeShadow)&&"adoptedStyleSheets"in Document.prototype&&"replace"in CSSStyleSheet.prototype,Fe=Symbol(),Xe=new WeakMap;let Ct=class{constructor(e,i,s){if(this._$cssResult$=!0,s!==Fe)throw Error("CSSResult is not constructable. Use `unsafeCSS` or `css` instead.");this.cssText=e,this.t=i}get styleSheet(){let e=this.o;const i=this.t;if(qe&&e===void 0){const s=i!==void 0&&i.length===1;s&&(e=Xe.get(i)),e===void 0&&((this.o=e=new CSSStyleSheet).replaceSync(this.cssText),s&&Xe.set(i,e))}return e}toString(){return this.cssText}};const ni=t=>new Ct(typeof t=="string"?t:t+"",void 0,Fe),ve=(t,...e)=>{const i=t.length===1?t[0]:e.reduce((s,a,n)=>s+(o=>{if(o._$cssResult$===!0)return o.cssText;if(typeof o=="number")return o;throw Error("Value passed to 'css' function must be a 'css' function result: "+o+". Use 'unsafeCSS' to pass non-literal values, but take care to ensure page security.")})(a)+t[n+1],t[0]);return new Ct(i,t,Fe)},ri=(t,e)=>{if(qe)t.adoptedStyleSheets=e.map(i=>i instanceof CSSStyleSheet?i:i.styleSheet);else for(const i of e){const s=document.createElement("style"),a=ce.litNonce;a!==void 0&&s.setAttribute("nonce",a),s.textContent=i.cssText,t.appendChild(s)}},et=qe?t=>t:t=>t instanceof CSSStyleSheet?(e=>{let i="";for(const s of e.cssRules)i+=s.cssText;return ni(i)})(t):t;const{is:oi,defineProperty:li,getOwnPropertyDescriptor:ci,getOwnPropertyNames:di,getOwnPropertySymbols:pi,getPrototypeOf:ui}=Object,ye=globalThis,tt=ye.trustedTypes,hi=tt?tt.emptyScript:"",gi=ye.reactiveElementPolyfillSupport,Y=(t,e)=>t,he={toAttribute(t,e){switch(e){case Boolean:t=t?hi:null;break;case Object:case Array:t=t==null?t:JSON.stringify(t)}return t},fromAttribute(t,e){let i=t;switch(e){case Boolean:i=t!==null;break;case Number:i=t===null?null:Number(t);break;case Object:case Array:try{i=JSON.parse(t)}catch{i=null}}return i}},ze=(t,e)=>!oi(t,e),it={attribute:!0,type:String,converter:he,reflect:!1,useDefault:!1,hasChanged:ze};Symbol.metadata??=Symbol("metadata"),ye.litPropertyMetadata??=new WeakMap;let F=class extends HTMLElement{static addInitializer(e){this._$Ei(),(this.l??=[]).push(e)}static get observedAttributes(){return this.finalize(),this._$Eh&&[...this._$Eh.keys()]}static createProperty(e,i=it){if(i.state&&(i.attribute=!1),this._$Ei(),this.prototype.hasOwnProperty(e)&&((i=Object.create(i)).wrapped=!0),this.elementProperties.set(e,i),!i.noAccessor){const s=Symbol(),a=this.getPropertyDescriptor(e,s,i);a!==void 0&&li(this.prototype,e,a)}}static getPropertyDescriptor(e,i,s){const{get:a,set:n}=ci(this.prototype,e)??{get(){return this[i]},set(o){this[i]=o}};return{get:a,set(o){const r=a?.call(this);n?.call(this,o),this.requestUpdate(e,r,s)},configurable:!0,enumerable:!0}}static getPropertyOptions(e){return this.elementProperties.get(e)??it}static _$Ei(){if(this.hasOwnProperty(Y("elementProperties")))return;const e=ui(this);e.finalize(),e.l!==void 0&&(this.l=[...e.l]),this.elementProperties=new Map(e.elementProperties)}static finalize(){if(this.hasOwnProperty(Y("finalized")))return;if(this.finalized=!0,this._$Ei(),this.hasOwnProperty(Y("properties"))){const i=this.properties,s=[...di(i),...pi(i)];for(const a of s)this.createProperty(a,i[a])}const e=this[Symbol.metadata];if(e!==null){const i=litPropertyMetadata.get(e);if(i!==void 0)for(const[s,a]of i)this.elementProperties.set(s,a)}this._$Eh=new Map;for(const[i,s]of this.elementProperties){const a=this._$Eu(i,s);a!==void 0&&this._$Eh.set(a,i)}this.elementStyles=this.finalizeStyles(this.styles)}static finalizeStyles(e){const i=[];if(Array.isArray(e)){const s=new Set(e.flat(1/0).reverse());for(const a of s)i.unshift(et(a))}else e!==void 0&&i.push(et(e));return i}static _$Eu(e,i){const s=i.attribute;return s===!1?void 0:typeof s=="string"?s:typeof e=="string"?e.toLowerCase():void 0}constructor(){super(),this._$Ep=void 0,this.isUpdatePending=!1,this.hasUpdated=!1,this._$Em=null,this._$Ev()}_$Ev(){this._$ES=new Promise(e=>this.enableUpdating=e),this._$AL=new Map,this._$E_(),this.requestUpdate(),this.constructor.l?.forEach(e=>e(this))}addController(e){(this._$EO??=new Set).add(e),this.renderRoot!==void 0&&this.isConnected&&e.hostConnected?.()}removeController(e){this._$EO?.delete(e)}_$E_(){const e=new Map,i=this.constructor.elementProperties;for(const s of i.keys())this.hasOwnProperty(s)&&(e.set(s,this[s]),delete this[s]);e.size>0&&(this._$Ep=e)}createRenderRoot(){const e=this.shadowRoot??this.attachShadow(this.constructor.shadowRootOptions);return ri(e,this.constructor.elementStyles),e}connectedCallback(){this.renderRoot??=this.createRenderRoot(),this.enableUpdating(!0),this._$EO?.forEach(e=>e.hostConnected?.())}enableUpdating(e){}disconnectedCallback(){this._$EO?.forEach(e=>e.hostDisconnected?.())}attributeChangedCallback(e,i,s){this._$AK(e,s)}_$ET(e,i){const s=this.constructor.elementProperties.get(e),a=this.constructor._$Eu(e,s);if(a!==void 0&&s.reflect===!0){const n=(s.converter?.toAttribute!==void 0?s.converter:he).toAttribute(i,s.type);this._$Em=e,n==null?this.removeAttribute(a):this.setAttribute(a,n),this._$Em=null}}_$AK(e,i){const s=this.constructor,a=s._$Eh.get(e);if(a!==void 0&&this._$Em!==a){const n=s.getPropertyOptions(a),o=typeof n.converter=="function"?{fromAttribute:n.converter}:n.converter?.fromAttribute!==void 0?n.converter:he;this._$Em=a;const r=o.fromAttribute(i,n.type);this[a]=r??this._$Ej?.get(a)??r,this._$Em=null}}requestUpdate(e,i,s,a=!1,n){if(e!==void 0){const o=this.constructor;if(a===!1&&(n=this[e]),s??=o.getPropertyOptions(e),!((s.hasChanged??ze)(n,i)||s.useDefault&&s.reflect&&n===this._$Ej?.get(e)&&!this.hasAttribute(o._$Eu(e,s))))return;this.C(e,i,s)}this.isUpdatePending===!1&&(this._$ES=this._$EP())}C(e,i,{useDefault:s,reflect:a,wrapped:n},o){s&&!(this._$Ej??=new Map).has(e)&&(this._$Ej.set(e,o??i??this[e]),n!==!0||o!==void 0)||(this._$AL.has(e)||(this.hasUpdated||s||(i=void 0),this._$AL.set(e,i)),a===!0&&this._$Em!==e&&(this._$Eq??=new Set).add(e))}async _$EP(){this.isUpdatePending=!0;try{await this._$ES}catch(i){Promise.reject(i)}const e=this.scheduleUpdate();return e!=null&&await e,!this.isUpdatePending}scheduleUpdate(){return this.performUpdate()}performUpdate(){if(!this.isUpdatePending)return;if(!this.hasUpdated){if(this.renderRoot??=this.createRenderRoot(),this._$Ep){for(const[a,n]of this._$Ep)this[a]=n;this._$Ep=void 0}const s=this.constructor.elementProperties;if(s.size>0)for(const[a,n]of s){const{wrapped:o}=n,r=this[a];o!==!0||this._$AL.has(a)||r===void 0||this.C(a,void 0,n,r)}}let e=!1;const i=this._$AL;try{e=this.shouldUpdate(i),e?(this.willUpdate(i),this._$EO?.forEach(s=>s.hostUpdate?.()),this.update(i)):this._$EM()}catch(s){throw e=!1,this._$EM(),s}e&&this._$AE(i)}willUpdate(e){}_$AE(e){this._$EO?.forEach(i=>i.hostUpdated?.()),this.hasUpdated||(this.hasUpdated=!0,this.firstUpdated(e)),this.updated(e)}_$EM(){this._$AL=new Map,this.isUpdatePending=!1}get updateComplete(){return this.getUpdateComplete()}getUpdateComplete(){return this._$ES}shouldUpdate(e){return!0}update(e){this._$Eq&&=this._$Eq.forEach(i=>this._$ET(i,this[i])),this._$EM()}updated(e){}firstUpdated(e){}};F.elementStyles=[],F.shadowRootOptions={mode:"open"},F[Y("elementProperties")]=new Map,F[Y("finalized")]=new Map,gi?.({ReactiveElement:F}),(ye.reactiveElementVersions??=[]).push("2.1.2");const He=globalThis,st=t=>t,ge=He.trustedTypes,at=ge?ge.createPolicy("lit-html",{createHTML:t=>t}):void 0,Et="$lit$",O=`lit$${Math.random().toFixed(9).slice(2)}$`,Pt="?"+O,mi=`<${Pt}>`,L=document,X=()=>L.createComment(""),ee=t=>t===null||typeof t!="object"&&typeof t!="function",Je=Array.isArray,fi=t=>Je(t)||typeof t?.[Symbol.iterator]=="function",Te=`[ 	
\f\r]`,G=/<(?:(!--|\/[^a-zA-Z])|(\/?[a-zA-Z][^>\s]*)|(\/?$))/g,nt=/-->/g,rt=/>/g,j=RegExp(`>|${Te}(?:([^\\s"'>=/]+)(${Te}*=${Te}*(?:[^ 	
\f\r"'\`<>=]|("|')|))|$)`,"g"),ot=/'/g,lt=/"/g,Dt=/^(?:script|style|textarea|title)$/i,bi=t=>(e,...i)=>({_$litType$:t,strings:e,values:i}),c=bi(1),J=Symbol.for("lit-noChange"),g=Symbol.for("lit-nothing"),ct=new WeakMap,R=L.createTreeWalker(L,129);function It(t,e){if(!Je(t)||!t.hasOwnProperty("raw"))throw Error("invalid template strings array");return at!==void 0?at.createHTML(e):e}const vi=(t,e)=>{const i=t.length-1,s=[];let a,n=e===2?"<svg>":e===3?"<math>":"",o=G;for(let r=0;r<i;r++){const l=t[r];let d,p,u=-1,y=0;for(;y<l.length&&(o.lastIndex=y,p=o.exec(l),p!==null);)y=o.lastIndex,o===G?p[1]==="!--"?o=nt:p[1]!==void 0?o=rt:p[2]!==void 0?(Dt.test(p[2])&&(a=RegExp("</"+p[2],"g")),o=j):p[3]!==void 0&&(o=j):o===j?p[0]===">"?(o=a??G,u=-1):p[1]===void 0?u=-2:(u=o.lastIndex-p[2].length,d=p[1],o=p[3]===void 0?j:p[3]==='"'?lt:ot):o===lt||o===ot?o=j:o===nt||o===rt?o=G:(o=j,a=void 0);const x=o===j&&t[r+1].startsWith("/>")?" ":"";n+=o===G?l+mi:u>=0?(s.push(d),l.slice(0,u)+Et+l.slice(u)+O+x):l+O+(u===-2?r:x)}return[It(t,n+(t[i]||"<?>")+(e===2?"</svg>":e===3?"</math>":"")),s]};class te{constructor({strings:e,_$litType$:i},s){let a;this.parts=[];let n=0,o=0;const r=e.length-1,l=this.parts,[d,p]=vi(e,i);if(this.el=te.createElement(d,s),R.currentNode=this.el.content,i===2||i===3){const u=this.el.content.firstChild;u.replaceWith(...u.childNodes)}for(;(a=R.nextNode())!==null&&l.length<r;){if(a.nodeType===1){if(a.hasAttributes())for(const u of a.getAttributeNames())if(u.endsWith(Et)){const y=p[o++],x=a.getAttribute(u).split(O),k=/([.?@])?(.*)/.exec(y);l.push({type:1,index:n,name:k[2],strings:x,ctor:k[1]==="."?xi:k[1]==="?"?wi:k[1]==="@"?$i:xe}),a.removeAttribute(u)}else u.startsWith(O)&&(l.push({type:6,index:n}),a.removeAttribute(u));if(Dt.test(a.tagName)){const u=a.textContent.split(O),y=u.length-1;if(y>0){a.textContent=ge?ge.emptyScript:"";for(let x=0;x<y;x++)a.append(u[x],X()),R.nextNode(),l.push({type:2,index:++n});a.append(u[y],X())}}}else if(a.nodeType===8)if(a.data===Pt)l.push({type:2,index:n});else{let u=-1;for(;(u=a.data.indexOf(O,u+1))!==-1;)l.push({type:7,index:n}),u+=O.length-1}n++}}static createElement(e,i){const s=L.createElement("template");return s.innerHTML=e,s}}function V(t,e,i=t,s){if(e===J)return e;let a=s!==void 0?i._$Co?.[s]:i._$Cl;const n=ee(e)?void 0:e._$litDirective$;return a?.constructor!==n&&(a?._$AO?.(!1),n===void 0?a=void 0:(a=new n(t),a._$AT(t,i,s)),s!==void 0?(i._$Co??=[])[s]=a:i._$Cl=a),a!==void 0&&(e=V(t,a._$AS(t,e.values),a,s)),e}class yi{constructor(e,i){this._$AV=[],this._$AN=void 0,this._$AD=e,this._$AM=i}get parentNode(){return this._$AM.parentNode}get _$AU(){return this._$AM._$AU}u(e){const{el:{content:i},parts:s}=this._$AD,a=(e?.creationScope??L).importNode(i,!0);R.currentNode=a;let n=R.nextNode(),o=0,r=0,l=s[0];for(;l!==void 0;){if(o===l.index){let d;l.type===2?d=new ne(n,n.nextSibling,this,e):l.type===1?d=new l.ctor(n,l.name,l.strings,this,e):l.type===6&&(d=new ki(n,this,e)),this._$AV.push(d),l=s[++r]}o!==l?.index&&(n=R.nextNode(),o++)}return R.currentNode=L,a}p(e){let i=0;for(const s of this._$AV)s!==void 0&&(s.strings!==void 0?(s._$AI(e,s,i),i+=s.strings.length-2):s._$AI(e[i])),i++}}class ne{get _$AU(){return this._$AM?._$AU??this._$Cv}constructor(e,i,s,a){this.type=2,this._$AH=g,this._$AN=void 0,this._$AA=e,this._$AB=i,this._$AM=s,this.options=a,this._$Cv=a?.isConnected??!0}get parentNode(){let e=this._$AA.parentNode;const i=this._$AM;return i!==void 0&&e?.nodeType===11&&(e=i.parentNode),e}get startNode(){return this._$AA}get endNode(){return this._$AB}_$AI(e,i=this){e=V(this,e,i),ee(e)?e===g||e==null||e===""?(this._$AH!==g&&this._$AR(),this._$AH=g):e!==this._$AH&&e!==J&&this._(e):e._$litType$!==void 0?this.$(e):e.nodeType!==void 0?this.T(e):fi(e)?this.k(e):this._(e)}O(e){return this._$AA.parentNode.insertBefore(e,this._$AB)}T(e){this._$AH!==e&&(this._$AR(),this._$AH=this.O(e))}_(e){this._$AH!==g&&ee(this._$AH)?this._$AA.nextSibling.data=e:this.T(L.createTextNode(e)),this._$AH=e}$(e){const{values:i,_$litType$:s}=e,a=typeof s=="number"?this._$AC(e):(s.el===void 0&&(s.el=te.createElement(It(s.h,s.h[0]),this.options)),s);if(this._$AH?._$AD===a)this._$AH.p(i);else{const n=new yi(a,this),o=n.u(this.options);n.p(i),this.T(o),this._$AH=n}}_$AC(e){let i=ct.get(e.strings);return i===void 0&&ct.set(e.strings,i=new te(e)),i}k(e){Je(this._$AH)||(this._$AH=[],this._$AR());const i=this._$AH;let s,a=0;for(const n of e)a===i.length?i.push(s=new ne(this.O(X()),this.O(X()),this,this.options)):s=i[a],s._$AI(n),a++;a<i.length&&(this._$AR(s&&s._$AB.nextSibling,a),i.length=a)}_$AR(e=this._$AA.nextSibling,i){for(this._$AP?.(!1,!0,i);e!==this._$AB;){const s=st(e).nextSibling;st(e).remove(),e=s}}setConnected(e){this._$AM===void 0&&(this._$Cv=e,this._$AP?.(e))}}class xe{get tagName(){return this.element.tagName}get _$AU(){return this._$AM._$AU}constructor(e,i,s,a,n){this.type=1,this._$AH=g,this._$AN=void 0,this.element=e,this.name=i,this._$AM=a,this.options=n,s.length>2||s[0]!==""||s[1]!==""?(this._$AH=Array(s.length-1).fill(new String),this.strings=s):this._$AH=g}_$AI(e,i=this,s,a){const n=this.strings;let o=!1;if(n===void 0)e=V(this,e,i,0),o=!ee(e)||e!==this._$AH&&e!==J,o&&(this._$AH=e);else{const r=e;let l,d;for(e=n[0],l=0;l<n.length-1;l++)d=V(this,r[s+l],i,l),d===J&&(d=this._$AH[l]),o||=!ee(d)||d!==this._$AH[l],d===g?e=g:e!==g&&(e+=(d??"")+n[l+1]),this._$AH[l]=d}o&&!a&&this.j(e)}j(e){e===g?this.element.removeAttribute(this.name):this.element.setAttribute(this.name,e??"")}}class xi extends xe{constructor(){super(...arguments),this.type=3}j(e){this.element[this.name]=e===g?void 0:e}}class wi extends xe{constructor(){super(...arguments),this.type=4}j(e){this.element.toggleAttribute(this.name,!!e&&e!==g)}}class $i extends xe{constructor(e,i,s,a,n){super(e,i,s,a,n),this.type=5}_$AI(e,i=this){if((e=V(this,e,i,0)??g)===J)return;const s=this._$AH,a=e===g&&s!==g||e.capture!==s.capture||e.once!==s.once||e.passive!==s.passive,n=e!==g&&(s===g||a);a&&this.element.removeEventListener(this.name,this,s),n&&this.element.addEventListener(this.name,this,e),this._$AH=e}handleEvent(e){typeof this._$AH=="function"?this._$AH.call(this.options?.host??this.element,e):this._$AH.handleEvent(e)}}class ki{constructor(e,i,s){this.element=e,this.type=6,this._$AN=void 0,this._$AM=i,this.options=s}get _$AU(){return this._$AM._$AU}_$AI(e){V(this,e)}}const _i=He.litHtmlPolyfillSupport;_i?.(te,ne),(He.litHtmlVersions??=[]).push("3.3.3");const Ti=(t,e,i)=>{const s=i?.renderBefore??e;let a=s._$litPart$;if(a===void 0){const n=i?.renderBefore??null;s._$litPart$=a=new ne(e.insertBefore(X(),n),n,void 0,i??{})}return a._$AI(t),a};const Ve=globalThis;class M extends F{constructor(){super(...arguments),this.renderOptions={host:this},this._$Do=void 0}createRenderRoot(){const e=super.createRenderRoot();return this.renderOptions.renderBefore??=e.firstChild,e}update(e){const i=this.render();this.hasUpdated||(this.renderOptions.isConnected=this.isConnected),super.update(e),this._$Do=Ti(i,this.renderRoot,this.renderOptions)}connectedCallback(){super.connectedCallback(),this._$Do?.setConnected(!0)}disconnectedCallback(){super.disconnectedCallback(),this._$Do?.setConnected(!1)}render(){return J}}M._$litElement$=!0,M.finalized=!0,Ve.litElementHydrateSupport?.({LitElement:M});const Si=Ve.litElementPolyfillSupport;Si?.({LitElement:M});(Ve.litElementVersions??=[]).push("4.2.2");const Be=t=>(e,i)=>{i!==void 0?i.addInitializer(()=>{customElements.define(t,e)}):customElements.define(t,e)};const Ai={attribute:!0,type:String,converter:he,reflect:!1,hasChanged:ze},Ci=(t=Ai,e,i)=>{const{kind:s,metadata:a}=i;let n=globalThis.litPropertyMetadata.get(a);if(n===void 0&&globalThis.litPropertyMetadata.set(a,n=new Map),s==="setter"&&((t=Object.create(t)).wrapped=!0),n.set(i.name,t),s==="accessor"){const{name:o}=i;return{set(r){const l=e.get.call(this);e.set.call(this,r),this.requestUpdate(o,l,t,!0,r)},init(r){return r!==void 0&&this.C(o,void 0,t,r),r}}}if(s==="setter"){const{name:o}=i;return function(r){const l=this[o];e.call(this,r),this.requestUpdate(o,l,t,!0,r)}}throw Error("Unsupported decorator location: "+s)};function we(t){return(e,i)=>typeof i=="object"?Ci(t,e,i):((s,a,n)=>{const o=a.hasOwnProperty(n);return a.constructor.createProperty(n,s),o?Object.getOwnPropertyDescriptor(a,n):void 0})(t,e,i)}function f(t){return we({...t,state:!0,attribute:!1})}const Ot={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 4h4v16H6zm8 0h4v16h-4z"/>'},Nt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m5 3l14 9l-14 9V3z"/>'},ie={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 6h18m-2 0v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6m3 0V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2m-6 5v6m4-6v6"/>'},jt={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18 6L6 18M6 6l12 12"/>'},Ei={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></g>'};const Rt=Object.freeze({left:0,top:0,width:16,height:16}),me=Object.freeze({rotate:0,vFlip:!1,hFlip:!1}),re=Object.freeze({...Rt,...me}),De=Object.freeze({...re,body:"",hidden:!1}),Pi=Object.freeze({width:null,height:null}),Mt=Object.freeze({...Pi,...me});function Di(t,e=0){const i=t.replace(/^-?[0-9.]*/,"");function s(a){for(;a<0;)a+=4;return a%4}if(i===""){const a=parseInt(t);return isNaN(a)?0:s(a)}else if(i!==t){let a=0;switch(i){case"%":a=25;break;case"deg":a=90}if(a){let n=parseFloat(t.slice(0,t.length-i.length));return isNaN(n)?0:(n=n/a,n%1===0?s(n):0)}}return e}const Ii=/[\s,]+/;function Oi(t,e){e.split(Ii).forEach(i=>{switch(i.trim()){case"horizontal":t.hFlip=!0;break;case"vertical":t.vFlip=!0;break}})}const Lt={...Mt,preserveAspectRatio:""};function dt(t){const e={...Lt},i=(s,a)=>t.getAttribute(s)||a;return e.width=i("width",null),e.height=i("height",null),e.rotate=Di(i("rotate","")),Oi(e,i("flip","")),e.preserveAspectRatio=i("preserveAspectRatio",i("preserveaspectratio","")),e}function Ni(t,e){for(const i in Lt)if(t[i]!==e[i])return!0;return!1}const Ut=/^[a-z0-9]+(-[a-z0-9]+)*$/,oe=(t,e,i,s="")=>{const a=t.split(":");if(t.slice(0,1)==="@"){if(a.length<2||a.length>3)return null;s=a.shift().slice(1)}if(a.length>3||!a.length)return null;if(a.length>1){const r=a.pop(),l=a.pop(),d={provider:a.length>0?a[0]:s,prefix:l,name:r};return e&&!de(d)?null:d}const n=a[0],o=n.split("-");if(o.length>1){const r={provider:s,prefix:o.shift(),name:o.join("-")};return e&&!de(r)?null:r}if(i&&s===""){const r={provider:s,prefix:"",name:n};return e&&!de(r,i)?null:r}return null},de=(t,e)=>t?!!((e&&t.prefix===""||t.prefix)&&t.name):!1;function ji(t,e){const i=t.icons,s=t.aliases||Object.create(null),a=Object.create(null);function n(o){if(i[o])return a[o]=[];if(!(o in a)){a[o]=null;const r=s[o]&&s[o].parent,l=r&&n(r);l&&(a[o]=[r].concat(l))}return a[o]}return Object.keys(i).concat(Object.keys(s)).forEach(n),a}function Ri(t,e){const i={};!t.hFlip!=!e.hFlip&&(i.hFlip=!0),!t.vFlip!=!e.vFlip&&(i.vFlip=!0);const s=((t.rotate||0)+(e.rotate||0))%4;return s&&(i.rotate=s),i}function pt(t,e){const i=Ri(t,e);for(const s in De)s in me?s in t&&!(s in i)&&(i[s]=me[s]):s in e?i[s]=e[s]:s in t&&(i[s]=t[s]);return i}function Mi(t,e,i){const s=t.icons,a=t.aliases||Object.create(null);let n={};function o(r){n=pt(s[r]||a[r],n)}return o(e),i.forEach(o),pt(t,n)}function qt(t,e){const i=[];if(typeof t!="object"||typeof t.icons!="object")return i;t.not_found instanceof Array&&t.not_found.forEach(a=>{e(a,null),i.push(a)});const s=ji(t);for(const a in s){const n=s[a];n&&(e(a,Mi(t,a,n)),i.push(a))}return i}const Li={provider:"",aliases:{},not_found:{},...Rt};function Se(t,e){for(const i in e)if(i in t&&typeof t[i]!=typeof e[i])return!1;return!0}function Ft(t){if(typeof t!="object"||t===null)return null;const e=t;if(typeof e.prefix!="string"||!t.icons||typeof t.icons!="object"||!Se(t,Li))return null;const i=e.icons;for(const a in i){const n=i[a];if(!a||typeof n.body!="string"||!Se(n,De))return null}const s=e.aliases||Object.create(null);for(const a in s){const n=s[a],o=n.parent;if(!a||typeof o!="string"||!i[o]&&!s[o]||!Se(n,De))return null}return e}const fe=Object.create(null);function Ui(t,e){return{provider:t,prefix:e,icons:Object.create(null),missing:new Set}}function D(t,e){const i=fe[t]||(fe[t]=Object.create(null));return i[e]||(i[e]=Ui(t,e))}function zt(t,e){return Ft(e)?qt(e,(i,s)=>{s?t.icons[i]=s:t.missing.add(i)}):[]}function qi(t,e,i){try{if(typeof i.body=="string")return t.icons[e]={...i},!0}catch{}return!1}function Fi(t,e){let i=[];return(typeof t=="string"?[t]:Object.keys(fe)).forEach(s=>{(typeof s=="string"&&typeof e=="string"?[e]:Object.keys(fe[s]||{})).forEach(a=>{const n=D(s,a);i=i.concat(Object.keys(n.icons).map(o=>(s!==""?"@"+s+":":"")+a+":"+o))})}),i}let se=!1;function Ht(t){return typeof t=="boolean"&&(se=t),se}function ae(t){const e=typeof t=="string"?oe(t,!0,se):t;if(e){const i=D(e.provider,e.prefix),s=e.name;return i.icons[s]||(i.missing.has(s)?null:void 0)}}function Jt(t,e){const i=oe(t,!0,se);if(!i)return!1;const s=D(i.provider,i.prefix);return e?qi(s,i.name,e):(s.missing.add(i.name),!0)}function ut(t,e){if(typeof t!="object")return!1;if(typeof e!="string"&&(e=t.provider||""),se&&!e&&!t.prefix){let s=!1;return Ft(t)&&(t.prefix="",qt(t,(a,n)=>{Jt(a,n)&&(s=!0)})),s}const i=t.prefix;return de({prefix:i,name:"a"})?!!zt(D(e,i),t):!1}function zi(t){return!!ae(t)}function Hi(t){const e=ae(t);return e&&{...re,...e}}function Vt(t,e){t.forEach(i=>{const s=i.loaderCallbacks;s&&(i.loaderCallbacks=s.filter(a=>a.id!==e))})}function Ji(t){t.pendingCallbacksFlag||(t.pendingCallbacksFlag=!0,setTimeout(()=>{t.pendingCallbacksFlag=!1;const e=t.loaderCallbacks?t.loaderCallbacks.slice(0):[];if(!e.length)return;let i=!1;const s=t.provider,a=t.prefix;e.forEach(n=>{const o=n.icons,r=o.pending.length;o.pending=o.pending.filter(l=>{if(l.prefix!==a)return!0;const d=l.name;if(t.icons[d])o.loaded.push({provider:s,prefix:a,name:d});else if(t.missing.has(d))o.missing.push({provider:s,prefix:a,name:d});else return i=!0,!0;return!1}),o.pending.length!==r&&(i||Vt([t],n.id),n.callback(o.loaded.slice(0),o.missing.slice(0),o.pending.slice(0),n.abort))})}))}let Vi=0;function Bi(t,e,i){const s=Vi++,a=Vt.bind(null,i,s);if(!e.pending.length)return a;const n={id:s,icons:e,callback:t,abort:a};return i.forEach(o=>{(o.loaderCallbacks||(o.loaderCallbacks=[])).push(n)}),a}function Ki(t){const e={loaded:[],missing:[],pending:[]},i=Object.create(null);t.sort((a,n)=>a.provider!==n.provider?a.provider.localeCompare(n.provider):a.prefix!==n.prefix?a.prefix.localeCompare(n.prefix):a.name.localeCompare(n.name));let s={provider:"",prefix:"",name:""};return t.forEach(a=>{if(s.name===a.name&&s.prefix===a.prefix&&s.provider===a.provider)return;s=a;const n=a.provider,o=a.prefix,r=a.name,l=i[n]||(i[n]=Object.create(null)),d=l[o]||(l[o]=D(n,o));let p;r in d.icons?p=e.loaded:o===""||d.missing.has(r)?p=e.missing:p=e.pending;const u={provider:n,prefix:o,name:r};p.push(u)}),e}const Ie=Object.create(null);function ht(t,e){Ie[t]=e}function Oe(t){return Ie[t]||Ie[""]}function Gi(t,e=!0,i=!1){const s=[];return t.forEach(a=>{const n=typeof a=="string"?oe(a,e,i):a;n&&s.push(n)}),s}function Ke(t){let e;if(typeof t.resources=="string")e=[t.resources];else if(e=t.resources,!(e instanceof Array)||!e.length)return null;return{resources:e,path:t.path||"/",maxURL:t.maxURL||500,rotate:t.rotate||750,timeout:t.timeout||5e3,random:t.random===!0,index:t.index||0,dataAfterTimeout:t.dataAfterTimeout!==!1}}const $e=Object.create(null),W=["https://api.simplesvg.com","https://api.unisvg.com"],pe=[];for(;W.length>0;)W.length===1||Math.random()>.5?pe.push(W.shift()):pe.push(W.pop());$e[""]=Ke({resources:["https://api.iconify.design"].concat(pe)});function gt(t,e){const i=Ke(e);return i===null?!1:($e[t]=i,!0)}function ke(t){return $e[t]}function Wi(){return Object.keys($e)}const Qi={resources:[],index:0,timeout:2e3,rotate:750,random:!1,dataAfterTimeout:!1};function Yi(t,e,i,s){const a=t.resources.length,n=t.random?Math.floor(Math.random()*a):t.index;let o;if(t.random){let w=t.resources.slice(0);for(o=[];w.length>1;){const T=Math.floor(Math.random()*w.length);o.push(w[T]),w=w.slice(0,T).concat(w.slice(T+1))}o=o.concat(w)}else o=t.resources.slice(n).concat(t.resources.slice(0,n));const r=Date.now();let l="pending",d=0,p,u=null,y=[],x=[];typeof s=="function"&&x.push(s);function k(){u&&(clearTimeout(u),u=null)}function C(){l==="pending"&&(l="aborted"),k(),y.forEach(w=>{w.status==="pending"&&(w.status="aborted")}),y=[]}function $(w,T){T&&(x=[]),typeof w=="function"&&x.push(w)}function K(){return{startTime:r,payload:e,status:l,queriesSent:d,queriesPending:y.length,subscribe:$,abort:C}}function E(){l="failed",x.forEach(w=>{w(void 0,p)})}function S(){y.forEach(w=>{w.status==="pending"&&(w.status="aborted")}),y=[]}function _(w,T,I){const A=T!=="success";switch(y=y.filter(h=>h!==w),l){case"pending":break;case"failed":if(A||!t.dataAfterTimeout)return;break;default:return}if(T==="abort"){p=I,E();return}if(A){p=I,y.length||(o.length?q():E());return}if(k(),S(),!t.random){const h=t.resources.indexOf(w.resource);h!==-1&&h!==t.index&&(t.index=h)}l="completed",x.forEach(h=>{h(I)})}function q(){if(l!=="pending")return;k();const w=o.shift();if(w===void 0){if(y.length){u=setTimeout(()=>{k(),l==="pending"&&(S(),E())},t.timeout);return}E();return}const T={status:"pending",resource:w,callback:(I,A)=>{_(T,I,A)}};y.push(T),d++,u=setTimeout(q,t.rotate),i(w,e,T.callback)}return setTimeout(q),K}function Bt(t){const e={...Qi,...t};let i=[];function s(){i=i.filter(o=>o().status==="pending")}function a(o,r,l){const d=Yi(e,o,r,(p,u)=>{s(),l&&l(p,u)});return i.push(d),d}function n(o){return i.find(r=>o(r))||null}return{query:a,find:n,setIndex:o=>{e.index=o},getIndex:()=>e.index,cleanup:s}}function mt(){}const Ae=Object.create(null);function Zi(t){if(!Ae[t]){const e=ke(t);if(!e)return;Ae[t]={config:e,redundancy:Bt(e)}}return Ae[t]}function Kt(t,e,i){let s,a;if(typeof t=="string"){const n=Oe(t);if(!n)return i(void 0,424),mt;a=n.send;const o=Zi(t);o&&(s=o.redundancy)}else{const n=Ke(t);if(n){s=Bt(n);const o=Oe(t.resources?t.resources[0]:"");o&&(a=o.send)}}return!s||!a?(i(void 0,424),mt):s.query(e,a,i)().abort}function ft(){}function Xi(t){t.iconsLoaderFlag||(t.iconsLoaderFlag=!0,setTimeout(()=>{t.iconsLoaderFlag=!1,Ji(t)}))}function es(t){const e=[],i=[];return t.forEach(s=>{(s.match(Ut)?e:i).push(s)}),{valid:e,invalid:i}}function Q(t,e,i){function s(){const a=t.pendingIcons;e.forEach(n=>{a&&a.delete(n),t.icons[n]||t.missing.add(n)})}if(i&&typeof i=="object")try{if(!zt(t,i).length){s();return}}catch(a){console.error(a)}s(),Xi(t)}function bt(t,e){t instanceof Promise?t.then(i=>{e(i)}).catch(()=>{e(null)}):e(t)}function ts(t,e){t.iconsToLoad?t.iconsToLoad=t.iconsToLoad.concat(e).sort():t.iconsToLoad=e,t.iconsQueueFlag||(t.iconsQueueFlag=!0,setTimeout(()=>{t.iconsQueueFlag=!1;const{provider:i,prefix:s}=t,a=t.iconsToLoad;if(delete t.iconsToLoad,!a||!a.length)return;const n=t.loadIcon;if(t.loadIcons&&(a.length>1||!n)){bt(t.loadIcons(a,s,i),d=>{Q(t,a,d)});return}if(n){a.forEach(d=>{bt(n(d,s,i),p=>{Q(t,[d],p?{prefix:s,icons:{[d]:p}}:null)})});return}const{valid:o,invalid:r}=es(a);if(r.length&&Q(t,r,null),!o.length)return;const l=s.match(Ut)?Oe(i):null;if(!l){Q(t,o,null);return}l.prepare(i,s,o).forEach(d=>{Kt(i,d,p=>{Q(t,d.icons,p)})})}))}const Ge=(t,e)=>{const i=Ki(Gi(t,!0,Ht()));if(!i.pending.length){let r=!0;return e&&setTimeout(()=>{r&&e(i.loaded,i.missing,i.pending,ft)}),()=>{r=!1}}const s=Object.create(null),a=[];let n,o;return i.pending.forEach(r=>{const{provider:l,prefix:d}=r;if(d===o&&l===n)return;n=l,o=d,a.push(D(l,d));const p=s[l]||(s[l]=Object.create(null));p[d]||(p[d]=[])}),i.pending.forEach(r=>{const{provider:l,prefix:d,name:p}=r,u=D(l,d),y=u.pendingIcons||(u.pendingIcons=new Set);y.has(p)||(y.add(p),s[l][d].push(p))}),a.forEach(r=>{const l=s[r.provider][r.prefix];l.length&&ts(r,l)}),e?Bi(e,i,a):ft},is=t=>new Promise((e,i)=>{const s=typeof t=="string"?oe(t,!0):t;if(!s){i(t);return}Ge([s||t],a=>{if(a.length&&s){const n=ae(s);if(n){e({...re,...n});return}}i(t)})});function vt(t){try{const e=typeof t=="string"?JSON.parse(t):t;if(typeof e.body=="string")return{...e}}catch{}}function ss(t,e){if(typeof t=="object")return{data:vt(t),value:t};if(typeof t!="string")return{value:t};if(t.includes("{")){const n=vt(t);if(n)return{data:n,value:t}}const i=oe(t,!0,!0);if(!i)return{value:t};const s=ae(i);if(s!==void 0||!i.prefix)return{value:t,name:i,data:s};const a=Ge([i],()=>e(t,i,ae(i)));return{value:t,name:i,loading:a}}let Gt=!1;try{Gt=navigator.vendor.indexOf("Apple")===0}catch{}function as(t,e){switch(e){case"svg":case"bg":case"mask":return e}return e!=="style"&&(Gt||t.indexOf("<a")===-1)?"svg":t.indexOf("currentColor")===-1?"bg":"mask"}const ns=/(-?[0-9.]*[0-9]+[0-9.]*)/g,rs=/^-?[0-9.]*[0-9]+[0-9.]*$/g;function Ne(t,e,i){if(e===1)return t;if(i=i||100,typeof t=="number")return Math.ceil(t*e*i)/i;if(typeof t!="string")return t;const s=t.split(ns);if(s===null||!s.length)return t;const a=[];let n=s.shift(),o=rs.test(n);for(;;){if(o){const r=parseFloat(n);isNaN(r)?a.push(n):a.push(Math.ceil(r*e*i)/i)}else a.push(n);if(n=s.shift(),n===void 0)return a.join("");o=!o}}function os(t,e="defs"){let i="";const s=t.indexOf("<"+e);for(;s>=0;){const a=t.indexOf(">",s),n=t.indexOf("</"+e);if(a===-1||n===-1)break;const o=t.indexOf(">",n);if(o===-1)break;i+=t.slice(a+1,n).trim(),t=t.slice(0,s).trim()+t.slice(o+1)}return{defs:i,content:t}}function ls(t,e){return t?"<defs>"+t+"</defs>"+e:e}function cs(t,e,i){const s=os(t);return ls(s.defs,e+s.content+i)}const ds=t=>t==="unset"||t==="undefined"||t==="none";function Wt(t,e){const i={...re,...t},s={...Mt,...e},a={left:i.left,top:i.top,width:i.width,height:i.height};let n=i.body;[i,s].forEach(C=>{const $=[],K=C.hFlip,E=C.vFlip;let S=C.rotate;K?E?S+=2:($.push("translate("+(a.width+a.left).toString()+" "+(0-a.top).toString()+")"),$.push("scale(-1 1)"),a.top=a.left=0):E&&($.push("translate("+(0-a.left).toString()+" "+(a.height+a.top).toString()+")"),$.push("scale(1 -1)"),a.top=a.left=0);let _;switch(S<0&&(S-=Math.floor(S/4)*4),S=S%4,S){case 1:_=a.height/2+a.top,$.unshift("rotate(90 "+_.toString()+" "+_.toString()+")");break;case 2:$.unshift("rotate(180 "+(a.width/2+a.left).toString()+" "+(a.height/2+a.top).toString()+")");break;case 3:_=a.width/2+a.left,$.unshift("rotate(-90 "+_.toString()+" "+_.toString()+")");break}S%2===1&&(a.left!==a.top&&(_=a.left,a.left=a.top,a.top=_),a.width!==a.height&&(_=a.width,a.width=a.height,a.height=_)),$.length&&(n=cs(n,'<g transform="'+$.join(" ")+'">',"</g>"))});const o=s.width,r=s.height,l=a.width,d=a.height;let p,u;o===null?(u=r===null?"1em":r==="auto"?d:r,p=Ne(u,l/d)):(p=o==="auto"?l:o,u=r===null?Ne(p,d/l):r==="auto"?d:r);const y={},x=(C,$)=>{ds($)||(y[C]=$.toString())};x("width",p),x("height",u);const k=[a.left,a.top,l,d];return y.viewBox=k.join(" "),{attributes:y,viewBox:k,body:n}}function We(t,e){let i=t.indexOf("xlink:")===-1?"":' xmlns:xlink="http://www.w3.org/1999/xlink"';for(const s in e)i+=" "+s+'="'+e[s]+'"';return'<svg xmlns="http://www.w3.org/2000/svg"'+i+">"+t+"</svg>"}function ps(t){return t.replace(/"/g,"'").replace(/%/g,"%25").replace(/#/g,"%23").replace(/</g,"%3C").replace(/>/g,"%3E").replace(/\s+/g," ")}function us(t){return"data:image/svg+xml,"+ps(t)}function Qt(t){return'url("'+us(t)+'")'}const hs=()=>{let t;try{if(t=fetch,typeof t=="function")return t}catch{}};let be=hs();function gs(t){be=t}function ms(){return be}function fs(t,e){const i=ke(t);if(!i)return 0;let s;if(!i.maxURL)s=0;else{let a=0;i.resources.forEach(o=>{a=Math.max(a,o.length)});const n=e+".json?icons=";s=i.maxURL-a-i.path.length-n.length}return s}function bs(t){return t===404}const vs=(t,e,i)=>{const s=[],a=fs(t,e),n="icons";let o={type:n,provider:t,prefix:e,icons:[]},r=0;return i.forEach((l,d)=>{r+=l.length+1,r>=a&&d>0&&(s.push(o),o={type:n,provider:t,prefix:e,icons:[]},r=l.length),o.icons.push(l)}),s.push(o),s};function ys(t){if(typeof t=="string"){const e=ke(t);if(e)return e.path}return"/"}const xs=(t,e,i)=>{if(!be){i("abort",424);return}let s=ys(e.provider);switch(e.type){case"icons":{const n=e.prefix,o=e.icons.join(","),r=new URLSearchParams({icons:o});s+=n+".json?"+r.toString();break}case"custom":{const n=e.uri;s+=n.slice(0,1)==="/"?n.slice(1):n;break}default:i("abort",400);return}let a=503;be(t+s).then(n=>{const o=n.status;if(o!==200){setTimeout(()=>{i(bs(o)?"abort":"next",o)});return}return a=501,n.json()}).then(n=>{if(typeof n!="object"||n===null){setTimeout(()=>{n===404?i("abort",n):i("next",a)});return}setTimeout(()=>{i("success",n)})}).catch(()=>{i("next",a)})},ws={prepare:vs,send:xs};function $s(t,e,i){D(i||"",e).loadIcons=t}function ks(t,e,i){D(i||"",e).loadIcon=t}const Ce="data-style";let Yt="";function _s(t){Yt=t}function yt(t,e){let i=Array.from(t.childNodes).find(s=>s.hasAttribute&&s.hasAttribute(Ce));i||(i=document.createElement("style"),i.setAttribute(Ce,Ce),t.appendChild(i)),i.textContent=":host{display:inline-block;vertical-align:"+(e?"-0.125em":"0")+"}span,svg{display:block;margin:auto}"+Yt}function Zt(){ht("",ws),Ht(!0);let t;try{t=window}catch{}if(t){if(t.IconifyPreload!==void 0){const i=t.IconifyPreload,s="Invalid IconifyPreload syntax.";typeof i=="object"&&i!==null&&(i instanceof Array?i:[i]).forEach(a=>{try{(typeof a!="object"||a===null||a instanceof Array||typeof a.icons!="object"||typeof a.prefix!="string"||!ut(a))&&console.error(s)}catch{console.error(s)}})}if(t.IconifyProviders!==void 0){const i=t.IconifyProviders;if(typeof i=="object"&&i!==null)for(const s in i){const a="IconifyProviders["+s+"] is invalid.";try{const n=i[s];if(typeof n!="object"||!n||n.resources===void 0)continue;gt(s,n)||console.error(a)}catch{console.error(a)}}}}return{iconLoaded:zi,getIcon:Hi,listIcons:Fi,addIcon:Jt,addCollection:ut,calculateSize:Ne,buildIcon:Wt,iconToHTML:We,svgToURL:Qt,loadIcons:Ge,loadIcon:is,addAPIProvider:gt,setCustomIconLoader:ks,setCustomIconsLoader:$s,appendCustomStyle:_s,_api:{getAPIConfig:ke,setAPIModule:ht,sendAPIQuery:Kt,setFetch:gs,getFetch:ms,listAPIProviders:Wi}}}const je={"background-color":"currentColor"},Xt={"background-color":"transparent"},xt={image:"var(--svg)",repeat:"no-repeat",size:"100% 100%"},wt={"-webkit-mask":je,mask:je,background:Xt};for(const t in wt){const e=wt[t];for(const i in xt)e[t+"-"+i]=xt[i]}function $t(t){return t?t+(t.match(/^[-0-9.]+$/)?"px":""):"inherit"}function Ts(t,e,i){const s=document.createElement("span");let a=t.body;a.indexOf("<a")!==-1&&(a+="<!-- "+Date.now()+" -->");const n=t.attributes,o=We(a,{...n,width:e.width+"",height:e.height+""}),r=Qt(o),l=s.style,d={"--svg":r,width:$t(n.width),height:$t(n.height),...i?je:Xt};for(const p in d)l.setProperty(p,d[p]);return s}let Z;function Ss(){try{Z=window.trustedTypes.createPolicy("iconify",{createHTML:t=>t})}catch{Z=null}}function As(t){return Z===void 0&&Ss(),Z?Z.createHTML(t):t}function Cs(t){const e=document.createElement("span"),i=t.attributes;let s="";i.width||(s="width: inherit;"),i.height||(s+="height: inherit;"),s&&(i.style=s);const a=We(t.body,i);return e.innerHTML=As(a),e.firstChild}function Re(t){return Array.from(t.childNodes).find(e=>{const i=e.tagName&&e.tagName.toUpperCase();return i==="SPAN"||i==="SVG"})}function kt(t,e){const i=e.icon.data,s=e.customisations,a=Wt(i,s);s.preserveAspectRatio&&(a.attributes.preserveAspectRatio=s.preserveAspectRatio);const n=e.renderedMode;let o;n==="svg"?o=Cs(a):o=Ts(a,{...re,...i},n==="mask");const r=Re(t);r?o.tagName==="SPAN"&&r.tagName===o.tagName?r.setAttribute("style",o.getAttribute("style")):t.replaceChild(o,r):t.appendChild(o)}function _t(t,e,i){const s=i&&(i.rendered?i:i.lastRender);return{rendered:!1,inline:e,icon:t,lastRender:s}}function Es(t="iconify-icon"){let e,i;try{e=window.customElements,i=window.HTMLElement}catch{return}if(!e||!i)return;const s=e.get(t);if(s)return s;const a=["icon","mode","inline","noobserver","width","height","rotate","flip"],n=class extends i{_shadowRoot;_initialised=!1;_state;_checkQueued=!1;_connected=!1;_observer=null;_visible=!0;constructor(){super();const r=this._shadowRoot=this.attachShadow({mode:"open"}),l=this.hasAttribute("inline");yt(r,l),this._state=_t({value:""},l),this._queueCheck()}connectedCallback(){this._connected=!0,this.startObserver()}disconnectedCallback(){this._connected=!1,this.stopObserver()}static get observedAttributes(){return a.slice(0)}attributeChangedCallback(r){switch(r){case"inline":{const l=this.hasAttribute("inline"),d=this._state;l!==d.inline&&(d.inline=l,yt(this._shadowRoot,l));break}case"noobserver":{this.hasAttribute("noobserver")?this.startObserver():this.stopObserver();break}default:this._queueCheck()}}get icon(){const r=this.getAttribute("icon");if(r&&r.slice(0,1)==="{")try{return JSON.parse(r)}catch{}return r}set icon(r){typeof r=="object"&&(r=JSON.stringify(r)),this.setAttribute("icon",r)}get inline(){return this.hasAttribute("inline")}set inline(r){r?this.setAttribute("inline","true"):this.removeAttribute("inline")}get observer(){return this.hasAttribute("observer")}set observer(r){r?this.setAttribute("observer","true"):this.removeAttribute("observer")}restartAnimation(){const r=this._state;if(r.rendered){const l=this._shadowRoot;if(r.renderedMode==="svg")try{l.lastChild.setCurrentTime(0);return}catch{}kt(l,r)}}get status(){const r=this._state;return r.rendered?"rendered":r.icon.data===null?"failed":"loading"}_queueCheck(){this._checkQueued||(this._checkQueued=!0,setTimeout(()=>{this._check()}))}_check(){if(!this._checkQueued)return;this._checkQueued=!1;const r=this._state,l=this.getAttribute("icon");if(l!==r.icon.value){this._iconChanged(l);return}if(!r.rendered||!this._visible)return;const d=this.getAttribute("mode"),p=dt(this);(r.attrMode!==d||Ni(r.customisations,p)||!Re(this._shadowRoot))&&this._renderIcon(r.icon,p,d)}_iconChanged(r){const l=ss(r,(d,p,u)=>{const y=this._state;if(y.rendered||this.getAttribute("icon")!==d)return;const x={value:d,name:p,data:u};x.data?this._gotIconData(x):y.icon=x});l.data?this._gotIconData(l):this._state=_t(l,this._state.inline,this._state)}_forceRender(){if(!this._visible){const r=Re(this._shadowRoot);r&&this._shadowRoot.removeChild(r);return}this._queueCheck()}_gotIconData(r){this._checkQueued=!1,this._renderIcon(r,dt(this),this.getAttribute("mode"))}_renderIcon(r,l,d){const p=as(r.data.body,d),u=this._state.inline;kt(this._shadowRoot,this._state={rendered:!0,icon:r,inline:u,customisations:l,attrMode:d,renderedMode:p})}startObserver(){if(!this._observer&&!this.hasAttribute("noobserver"))try{this._observer=new IntersectionObserver(r=>{const l=r.some(d=>d.isIntersecting);l!==this._visible&&(this._visible=l,this._forceRender())}),this._observer.observe(this)}catch{if(this._observer){try{this._observer.disconnect()}catch{}this._observer=null}}}stopObserver(){this._observer&&(this._observer.disconnect(),this._observer=null,this._visible=!0,this._connected&&this._forceRender())}};a.forEach(r=>{r in n.prototype||Object.defineProperty(n.prototype,r,{get:function(){return this.getAttribute(r)},set:function(l){l!==null?this.setAttribute(r,l):this.removeAttribute(r)}})});const o=Zt();for(const r in o)n[r]=n.prototype[r]=o[r];return e.define(t,n),n}const Ps=Es()||Zt(),{iconLoaded:pa,getIcon:ua,listIcons:ha,addIcon:ga,addCollection:ma,calculateSize:fa,buildIcon:ba,iconToHTML:va,svgToURL:ya,loadIcons:xa,loadIcon:wa,setCustomIconLoader:$a,setCustomIconsLoader:ka,addAPIProvider:_a,_api:Ta}=Ps;class Qe extends Error{constructor(e,i){super(i),this.status=e,this.name="ApiRequestError"}}async function m(t,e){const i=await fetch(t,{...e,headers:{...e?.body?{"content-type":"application/json"}:{},...e?.headers}});if(!i.ok){const s=await i.json().catch(()=>({error:i.statusText}));throw new Qe(i.status,s.error||i.statusText)}return i.status===204?void 0:i.json()}function Me(t,e,i=!1){if(e==="telegram"){const s=String(t.get("bot_token")??"");return{type:"telegram",name:t.get("name"),bot_token:i&&!s?void 0:s,chat_id:t.get("chat_id"),default:t.get("default")==="on"}}if(e==="smtp"){const s=String(t.get("username")??""),a=String(t.get("password")??"");return{type:"smtp",name:t.get("name"),host:t.get("host"),port:Number(t.get("port")),security:t.get("security"),username:s||void 0,password:a||void 0,from:t.get("from"),to:t.get("to"),default:t.get("default")==="on"}}return{type:"webhook",name:t.get("name"),url:t.get("url"),headers:i?void 0:{},default:t.get("default")==="on"}}function Le(t,e=[],i=!0,s=String(t.get("kind")??"http"),a=[]){const n=String(t.get("url")),o=s==="http"?n:`${s}://${n.replace(/^[a-z][a-z0-9+.-]*:\/\//i,"")}`;return{name:String(t.get("name")),kind:s,url:o,method:String(t.get("method")??"GET"),accepted_statuses:[{start:200,end:299}],follow_redirects:!0,max_redirects:5,interval_seconds:Number(t.get("interval")),timeout_seconds:Number(t.get("timeout")),failure_threshold:Number(t.get("failures")),locations:Number(t.get("locations")??1),headers:{},body:null,assertions:a,skip_tls_verification:!1,tls_ca_secret_id:Ee(t,"tls_ca_secret_id"),tls_client_certificate_secret_id:Ee(t,"tls_client_certificate_secret_id"),tls_client_private_key_secret_id:Ee(t,"tls_client_private_key_secret_id"),notification_channel_ids:e,use_default_channels:i}}function Ee(t,e){return String(t.get(e)??"")||null}var Ds=Object.defineProperty,Is=Object.getOwnPropertyDescriptor,B=(t,e,i,s)=>{for(var a=s>1?void 0:s?Is(e,i):e,n=t.length-1,o;n>=0;n--)(o=t[n])&&(a=(s?o(e,i,a):o(a))||a);return s&&a&&Ds(e,i,a),a};let N=class extends M{constructor(){super(...arguments),this.channelKind="webhook",this.channels=[],this.saving=!1,this.error=""}connectedCallback(){super.connectedCallback(),this.loadChannels()}updated(t){t.has("setup")&&this.loadChannels()}async loadChannels(){if(!(!this.setup?.cluster_ready||this.setup.phase!=="target"))try{this.channels=await m("/api/v1/channels")}catch(t){this.fail(t)}}submittedNodeName(){return this.shadowRoot?.querySelector("#setup-node-name")?.value.trim()??""}async createCluster(t){if(t.preventDefault(),!window.confirm("Create a new single-Node Cluster?"))return;const e=new FormData(t.currentTarget),i=String(e.get("admin_username")??"").trim(),s=String(e.get("admin_password")??"");await this.choose("/api/v1/setup/new-cluster",{node_name:this.submittedNodeName(),admin_username:i,admin_password:s},{username:i,password:s})}async joinCluster(t){t.preventDefault();const e=t.currentTarget,i=new FormData(e);await this.choose("/api/v1/cluster/join",{node_name:this.submittedNodeName(),join_link:String(i.get("join_link")??"").trim()})}async choose(t,e,i){this.saving=!0,this.error="";try{await m(t,{method:"POST",body:JSON.stringify(e)}),await this.waitForCluster(i)}catch(s){this.fail(s),this.saving=!1}}async waitForCluster(t){for(let e=0;e<120;e+=1){const{promise:i,resolve:s}=Promise.withResolvers();window.setTimeout(s,250),await i;try{t&&await m("/api/v1/auth/login",{method:"POST",body:JSON.stringify(t)});const a=await m("/api/v1/setup");if(a.cluster_ready){this.changed(a);return}}catch(a){if(!t&&a instanceof Qe&&a.status===401){window.location.assign("/");return}}}throw new Error("Cluster setup did not finish within 30 seconds")}async createChannel(t){t.preventDefault();const e=new FormData(t.currentTarget),i=Me(e,this.channelKind);await this.createResource("/api/v1/channels",i)}async createTarget(t){t.preventDefault();const e=new FormData(t.currentTarget),i=Le(e,e.getAll("channel_id").map(String));await this.createResource("/api/v1/targets",i)}async createResource(t,e){this.saving=!0;try{await m(t,{method:"POST",body:JSON.stringify(e)}),await this.next()}catch(i){this.fail(i),this.saving=!1}}async next(){this.saving=!0;try{this.changed(await m("/api/v1/setup/next",{method:"POST"}))}catch(t){this.fail(t),this.saving=!1}}changed(t){this.saving=!1,this.dispatchEvent(new CustomEvent("setup-changed",{detail:t,bubbles:!0,composed:!0}))}fail(t){this.error=t instanceof Error?t.message:String(t)}render(){return c`<section class="flow" aria-label="UpGrid setup">
      ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:g}
      ${this.setup.phase==="cluster"?this.renderCluster():this.setup.phase==="channel"?this.renderChannel():this.renderTarget()}
    </section>`}renderCluster(){return c`
      <span class="eyebrow">First-run setup</span><h1>Choose your Cluster</h1>
      <p class="lead">Review this Node’s name, then create a new Cluster or use an invitation to join one.</p>
      <div class="cluster-panel">
        <div class="cluster-identity">
          <label for="setup-node-name">Node name<input id="setup-node-name" .value=${this.setup.node_name} required /></label>
        </div>
        <form class="cluster-create" @submit=${this.createCluster}>
          <div class="cluster-copy"><h2>Start a new Cluster</h2><p>Create its first replicated administrator identity.</p></div>
          <div class="cluster-create-fields">
            <label>Administrator username<input name="admin_username" autocomplete="username" value="admin" required /></label>
            <label>Administrator password<input name="admin_password" type="password" minlength="12" autocomplete="new-password" required /></label>
          </div>
          <button type="submit" ?disabled=${this.saving}>${this.saving?"Setting up…":"Create new Cluster"}</button>
        </form>
        <div class="cluster-divider"><span>Or</span></div>
        <form class="cluster-join" @submit=${this.joinCluster}>
          <div class="cluster-copy"><h2>Join an existing Cluster</h2><p>Paste an <code>up://</code> Join Token from a current member.</p></div>
          <div class="cluster-join-fields">
            <label>Join Token<input name="join_link" type="url" pattern="up://.*" placeholder="up://node.example/token" autocomplete="off" required /></label>
            <button class="secondary" type="submit" ?disabled=${this.saving}>Join Cluster</button>
          </div>
        </form>
      </div>`}renderChannel(){return c`
      <span class="eyebrow">Optional · Step 2 of 3</span><h1>Add a notification channel</h1>
      <p class="lead">Send availability transitions to Telegram or a webhook. <span class="count">${this.setup.channel_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createChannel}>
        <label>Type<select name="type" @change=${t=>this.channelKind=t.target.value}><option value="webhook">Webhook</option><option value="telegram">Telegram</option></select></label>
        <label>Name<input name="name" placeholder="On-call" required /></label>
        ${this.channelKind==="webhook"?c`<label>Webhook URL<input name="url" type="url" placeholder="https://hooks.example.com/upgrid" required /></label>`:c`<label>Bot token<input name="bot_token" type="password" autocomplete="off" required /></label><label>Chat ID<input name="chat_id" required /></label>`}
        <label class="switch"><span>Use as default channel</span><input class="switch-control" name="default" type="checkbox" role="switch" checked /></label>
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and continue</button></div>
      </form></div>`}renderTarget(){return c`
      <span class="eyebrow">Optional · Step 3 of 3</span><h1>Monitor your first Target</h1>
      <p class="lead">Configure an HTTP endpoint now or continue to the dashboard. <span class="count">${this.setup.target_count} already configured</span></p>
      <div class="panel"><form class="choice" @submit=${this.createTarget}>
        <label>Name<input name="name" placeholder="Production API" required /></label>
        <label>URL<input name="url" type="url" placeholder="https://example.com/health" required /></label>
        <div class="row"><label>Method<input name="method" value="GET" required /></label><label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label></div>
        <div class="row"><label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label><label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label></div>
        ${this.channels.length?c`<fieldset><legend>Notification channels</legend>${this.channels.map(t=>c`<label class="checkbox-option"><span>${t.name}</span><input class="checkbox-control" name="channel_id" type="checkbox" value=${t.id} /></label>`)}</fieldset>`:c`<p class="meta">No notification channels are available.</p>`}
        <div class="actions"><button class="secondary" type="button" @click=${this.next} ?disabled=${this.saving}>Skip</button><button type="submit" ?disabled=${this.saving}>Create and finish</button></div>
      </form></div>`}};N.styles=ve`
    :host { display: block; }
    *, *::before, *::after { box-sizing: border-box; }
    .flow { width: min(760px, 100%); margin: 0 auto; }
    .eyebrow { color: var(--muted); font-size: 12px; letter-spacing: .16em; text-transform: uppercase; }
    h1 { margin: 5px 0 8px; font-size: clamp(30px, 5vw, 46px); letter-spacing: -.04em; }
    .lead { margin: 0 0 16px; color: var(--muted); font-size: 15px; }
    .panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .choice { display: grid; gap: 14px; padding: 22px; border-top: 1px solid var(--line); }
    .choice:first-child { border-top: 0; }
    .choice h2 { margin: 0; font-size: 17px; }
    .choice p { margin: -8px 0 0; color: var(--muted); }
    .cluster-panel { border: 1px solid var(--line); border-radius: 16px; background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); overflow: hidden; }
    .cluster-identity, .cluster-create, .cluster-join { padding: 18px; }
    .cluster-identity { border-bottom: 1px solid var(--line); }
    .cluster-create { display: grid; gap: 14px; }
    .cluster-create-fields { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .cluster-create button { justify-self: end; }
    .cluster-copy h2 { margin: 0; font-size: 17px; }
    .cluster-copy p { margin: 2px 0 0; color: var(--muted); }
    .cluster-divider { display: flex; align-items: center; gap: 12px; color: var(--muted); font-size: 11px; text-transform: uppercase; letter-spacing: .12em; }
    .cluster-divider::before, .cluster-divider::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .cluster-join { display: grid; gap: 10px; }
    .cluster-join-fields { display: grid; grid-template-columns: minmax(0, 1fr) auto; align-items: end; gap: 10px; }
    .cluster-join-fields label { min-width: 0; }
    .cluster-join-fields button { height: 44px; white-space: nowrap; }
    form { display: grid; gap: 13px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .switch-label { display: flex; min-width: 0; align-items: center; gap: 8px; }
    .switch-control { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border: 1px solid var(--line); border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch-control::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch-control:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch-control:checked::after { background: var(--button-text); transform: translateX(18px); }
    .checkbox-option { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .checkbox-control { width: 18px; min-height: 18px; height: 18px; flex: none; accent-color: var(--button-bg); cursor: pointer; }
    input:not([type="checkbox"]), select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font: inherit; font-size: 16px; transition: border-color 160ms ease, opacity 160ms ease; }
    input:not([type="checkbox"]):focus, select:focus { border-color: var(--focus); }
    button:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    .actions { display: flex; justify-content: flex-end; gap: 9px; margin-top: 5px; }
    button { display: inline-flex; min-height: 44px; align-items: center; justify-content: center; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; cursor: pointer; font: inherit; transition: background-color 160ms ease, border-color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    button:hover { border-color: var(--button-hover-border); }
    button:active { transform: translateY(1px); }
    button:disabled { cursor: not-allowed; opacity: .65; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .notice { margin-bottom: 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .count { display: inline-block; margin-top: 6px; color: var(--green); font-size: 12px; }
    @media (max-width: 620px) { .row, .cluster-create-fields, .cluster-join-fields { grid-template-columns: 1fr; } .cluster-create button, .cluster-join button { justify-self: end; } }
    @media (max-height: 650px) and (min-width: 621px) {
      h1 { margin: 2px 0 4px; font-size: 30px; }
      .lead { margin-bottom: 8px; font-size: 13px; }
      .cluster-identity, .cluster-create, .cluster-join { padding: 8px 14px; }
      .cluster-create { grid-template-columns: minmax(0, 1fr) auto; gap: 8px; }
      .cluster-create .cluster-copy { grid-column: 1 / -1; }
      .cluster-create button { align-self: end; }
      .cluster-copy p { display: none; }
      .cluster-join { grid-template-columns: auto minmax(0, 1fr); align-items: end; }
      input:not([type="checkbox"]), button { min-height: 38px; }
      .cluster-join-fields button { height: 44px; }
    }
  `;B([we({attribute:!1})],N.prototype,"setup",2);B([f()],N.prototype,"channelKind",2);B([f()],N.prototype,"channels",2);B([f()],N.prototype,"saving",2);B([f()],N.prototype,"error",2);N=B([Be("upgrid-setup")],N);var Os=Object.defineProperty,Ns=Object.getOwnPropertyDescriptor,_e=(t,e,i,s)=>{for(var a=s>1?void 0:s?Ns(e,i):e,n=t.length-1,o;n>=0;n--)(o=t[n])&&(a=(s?o(e,i,a):o(a))||a);return s&&a&&Os(e,i,a),a};const js={body_contains:"Body contains",body_regex:"Body regex",json_path:"JSONPath",response_header:"Response header",latency:"Latency threshold",script:"Script"};let U=class extends M{constructor(){super(...arguments),this.assertions=[],this.targetId="new",this.draft=[],this.loadedTarget="",this.internals=this.attachInternals()}get value(){return structuredClone(this.draft)}willUpdate(t){t.has("targetId")&&this.loadedTarget!==this.targetId&&(this.loadedTarget=this.targetId,this.draft=structuredClone(this.assertions))}updated(){this.internals.setFormValue(JSON.stringify(this.draft))}formResetCallback(){this.draft=structuredClone(this.assertions),this.internals.setFormValue(JSON.stringify(this.draft))}add(){this.draft=[...this.draft,Tt("body_contains")],this.changed()}removeAssertion(t){this.draft=this.draft.filter((e,i)=>i!==t),this.changed()}move(t,e){const i=t+e;if(i<0||i>=this.draft.length)return;const s=[...this.draft];[s[t],s[i]]=[s[i],s[t]],this.draft=s,this.changed()}setKind(t,e){const i=e.currentTarget.value;this.replace(t,Tt(i))}set(t,e,i){const s=i.currentTarget,a={...this.draft[t],[e]:e==="max_ms"?Number(s.value):s.value||null};this.replace(t,a)}replace(t,e){this.draft=this.draft.map((i,s)=>s===t?e:i),this.changed()}changed(){this.internals.setFormValue(JSON.stringify(this.draft)),this.dispatchEvent(new Event("input",{bubbles:!0,composed:!0}))}render(){return c`
      <div class="assertions">
        <button class="add" type="button" @click=${this.add}>Add assertion</button>
        ${this.draft.length?this.draft.map((t,e)=>this.renderAssertion(t,e)):c`<p class="empty">No assertions.</p>`}
      </div>
    `}renderAssertion(t,e){return c`
      <div class="assertion">
        <label>Type<select aria-label=${`Assertion ${e+1} type`} .value=${t.kind} @change=${i=>this.setKind(e,i)}>${Object.entries(js).map(([i,s])=>c`<option value=${i}>${s}</option>`)}</select></label>
        ${this.renderFields(t,e)}
        <div class="actions">
          <button type="button" aria-label=${`Move assertion ${e+1} up`} ?disabled=${e===0} @click=${()=>this.move(e,-1)}>Up</button>
          <button type="button" aria-label=${`Move assertion ${e+1} down`} ?disabled=${e===this.draft.length-1} @click=${()=>this.move(e,1)}>Down</button>
          <button type="button" aria-label=${`Remove assertion ${e+1}`} @click=${()=>this.removeAssertion(e)}>Remove</button>
        </div>
      </div>
    `}renderFields(t,e){switch(t.kind){case"body_contains":return c`<div class="fields single"><label>Required text<input aria-label=${`Assertion ${e+1} required text`} .value=${t.value} required @input=${i=>this.set(e,"value",i)} /></label></div>`;case"body_regex":return c`<div class="fields single"><label>Regular expression<input aria-label=${`Assertion ${e+1} regular expression`} .value=${t.pattern} required @input=${i=>this.set(e,"pattern",i)} /></label></div>`;case"json_path":return c`<div class="fields"><label>Path<input aria-label=${`Assertion ${e+1} JSONPath`} .value=${t.path} placeholder="$.status" required @input=${i=>this.set(e,"path",i)} /></label><label>Expected value (optional)<input aria-label=${`Assertion ${e+1} expected value`} .value=${t.expected??""} @input=${i=>this.set(e,"expected",i)} /></label></div>`;case"response_header":return c`<div class="fields"><label>Header name<input aria-label=${`Assertion ${e+1} header name`} .value=${t.name} placeholder="content-type" required @input=${i=>this.set(e,"name",i)} /></label><label>Exact value (optional)<input aria-label=${`Assertion ${e+1} header value`} .value=${t.value??""} @input=${i=>this.set(e,"value",i)} /></label></div>`;case"latency":return c`<div class="fields single"><label>Maximum milliseconds<input aria-label=${`Assertion ${e+1} maximum milliseconds`} type="number" min="1" .value=${String(t.max_ms)} required @input=${i=>this.set(e,"max_ms",i)} /></label></div>`;case"script":return c`<div class="fields single"><label>Boolean Rhai expression<textarea aria-label=${`Assertion ${e+1} script`} required @input=${i=>this.set(e,"source",i)}>${t.source}</textarea></label></div>`;default:return g}}};U.formAssociated=!0;U.styles=ve`
    :host { display: grid; gap: 10px; }
    .assertions { display: grid; gap: 10px; }
    .assertion { display: grid; grid-template-columns: minmax(140px, 0.7fr) minmax(180px, 1.3fr) auto; gap: 8px; align-items: end; }
    .fields { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .fields.single { grid-template-columns: 1fr; }
    label { display: grid; gap: 5px; color: var(--muted); font-size: 12px; }
    input, select, textarea { box-sizing: border-box; width: 100%; border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 9px; font: inherit; }
    textarea { min-height: 72px; resize: vertical; font-family: ui-monospace, monospace; }
    .actions { display: flex; gap: 4px; }
    button { border: 1px solid var(--line); border-radius: 7px; background: var(--panel-2); color: var(--text); padding: 8px 10px; cursor: pointer; user-select: none; }
    button:disabled { cursor: not-allowed; opacity: 0.45; }
    .add { min-height: 44px; justify-self: start; border-color: var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .add:hover { border-color: var(--button-hover-border); }
    .add:active { transform: translateY(1px); }
    .empty { margin: 0; color: var(--muted); font-size: 13px; }
    @media (max-width: 720px) { .assertion { grid-template-columns: 1fr; } .fields { grid-template-columns: 1fr; } }
  `;_e([we({attribute:!1})],U.prototype,"assertions",2);_e([we({attribute:"target-id"})],U.prototype,"targetId",2);_e([f()],U.prototype,"draft",2);U=_e([Be("http-assertion-editor")],U);function Tt(t){switch(t){case"body_contains":return{kind:t,value:""};case"body_regex":return{kind:t,pattern:""};case"json_path":return{kind:t,path:"$",expected:null};case"response_header":return{kind:t,name:"",value:null};case"latency":return{kind:t,max_ms:1e3};case"script":return{kind:t,source:"status == 200"}}}const Rs={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 3a6 6 0 0 0 9 9a9 9 0 1 1-9-9Z"/>'},Ms={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><rect width="20" height="14" x="2" y="3" rx="2"/><path d="M8 21h8m-4-4v4"/></g>'},Ls={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M4.93 4.93l1.41 1.41m11.32 11.32l1.41 1.41M2 12h2m16 0h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/></g>'};var Us=Object.defineProperty,v=(t,e,i,s)=>{for(var a=void 0,n=t.length-1,o;n>=0;n--)(o=t[n])&&(a=o(e,i,a)||a);return a&&Us(e,i,a),a};const ue=["system","dark","bright"],St={system:Ms,dark:Rs,bright:Ls},z={overview:"/",alerts:"/alerts",cluster:"/cluster",trash:"/trash",changePassword:"/admin/change-password",users:"/admin/users",apiTokens:"/admin/api-tokens"};function qs(t,e){if(!e)return{tone:"pending",label:"connecting"};const i=t.filter(a=>!a.paused);if(!i.length)return{tone:"pending",label:"ready"};const s=i.filter(a=>a.availability==="down"||a.consecutive_failures>0).length;return s?s===i.length?{tone:"down",label:"down"}:{tone:"degraded",label:"partially down"}:{tone:"up",label:"up"}}function At(){return Object.entries(z).find(([,t])=>t===window.location.pathname)?.[0]??"overview"}function Fs(){const t=localStorage.getItem("upgrid-theme");return ue.includes(t)?t:"system"}class b extends M{constructor(){super(...arguments),this.targets=[],this.trashedTargets=[],this.channels=[],this.alerts=[],this.transitions=[],this.secrets=[],this.joinTokens=[],this.identities=[],this.apiTokens=[],this.authReady=!1,this.newApiToken="",this.error="",this.live=!1,this.saving=!1,this.historyLoading=!1,this.channelKind="webhook",this.channelTestMessage="",this.testingChannel=!1,this.joinCommand="",this.alertSearch="",this.alertDeliveryFilter="all",this.alertKindFilter="all",this.alertAcknowledgedFilter="all",this.search="",this.statusFilter="all",this.sort="name",this.selectedIds=new Set,this.activeSection=At(),this.copied=!1,this.setupMode=!1,this.warningDismissed=sessionStorage.getItem("upgrid-warning-dismissed")==="1",this.unlimitedUses=!1,this.theme=Fs(),this.detailDirty=!1,this.detailTab="details",this.detailInitialState="",this.systemTheme=matchMedia("(prefers-color-scheme: light)"),this.systemThemeChanged=()=>{this.theme==="system"&&this.applyTheme()},this.routeChanged=()=>{if(this.setupMode&&this.setup){window.history.replaceState(null,"",this.setup.path);return}this.activeSection=At()},this.backgroundClicked=e=>{const i=this.renderRoot.querySelector(".account-menu");i?.open&&!e.composedPath().includes(i)&&(i.open=!1)}}connectedCallback(){super.connectedCallback(),this.applyTheme(),this.systemTheme.addEventListener("change",this.systemThemeChanged),window.addEventListener("popstate",this.routeChanged),document.addEventListener("pointerdown",this.backgroundClicked),this.start()}disconnectedCallback(){this.systemTheme.removeEventListener("change",this.systemThemeChanged),window.removeEventListener("popstate",this.routeChanged),document.removeEventListener("pointerdown",this.backgroundClicked),this.events?.close(),super.disconnectedCallback()}async start(){try{const e=await m("/api/v1/setup");e.cluster_ready&&(this.session=await m("/api/v1/auth/session")),await this.activate(e)}catch(e){(!(e instanceof Qe)||e.status!==401)&&(this.error=e instanceof Error?e.message:String(e))}this.authReady=!0}async activate(e){if(this.setup=e,this.setupMode=e.setup,this.setupMode){window.history.replaceState(null,"",e.path),e.cluster_ready?(await this.refresh(),this.connectEvents()):this.live=!0;return}await this.refresh(),this.connectEvents()}async login(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0,this.error="";try{this.session=await m("/api/v1/auth/login",{method:"POST",body:JSON.stringify({username:String(i.get("username")??""),password:String(i.get("password")??"")})}),await this.activate(await m("/api/v1/setup"))}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}async logout(){await m("/api/v1/auth/logout",{method:"POST"}),this.events?.close(),this.session=void 0,this.live=!1,this.setupMode=!1,window.history.replaceState(null,"","/")}connectEvents(){this.events?.close(),this.events=new EventSource("/api/v1/events"),this.events.addEventListener("state",()=>{this.refresh()}),this.events.onopen=()=>this.live=!0,this.events.onerror=()=>this.live=!1}applyTheme(){const e=this.theme==="system"?this.systemTheme.matches?"bright":"dark":this.theme;this.dataset.theme=e,document.querySelector('meta[name="theme-color"]')?.setAttribute("content",e==="bright"?"#f4f8f6":"#0b1110")}cycleTheme(){this.theme=ue[(ue.indexOf(this.theme)+1)%ue.length],localStorage.setItem("upgrid-theme",this.theme),this.applyTheme()}dismissWarning(){sessionStorage.setItem("upgrid-warning-dismissed","1"),this.warningDismissed=!0}async refresh(){try{[this.targets,this.trashedTargets,this.channels,this.alerts,this.transitions,this.secrets,this.cluster,this.joinTokens,this.identities,this.apiTokens]=await Promise.all([m("/api/v1/targets"),m("/api/v1/trash/targets"),m("/api/v1/channels"),m("/api/v1/alerts"),m("/api/v1/transitions"),m("/api/v1/secrets"),m("/api/v1/cluster"),m("/api/v1/join-tokens"),m("/api/v1/identities"),m("/api/v1/api-tokens")]),this.error=""}catch(e){this.error=e instanceof Error?e.message:String(e)}}openTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.showModal()}closeTargetDialog(){this.renderRoot.querySelector("#target-dialog")?.close()}openTarget(e){this.detailDirty=!1,this.detailTab="details",this.selected=e,this.targetHistory=void 0,this.historyLoading=!0,this.loadTargetHistory(e.id),this.updateComplete.then(()=>{const i=this.renderRoot.querySelector("#detail-dialog"),s=i?.querySelector("form");s&&(this.detailInitialState=this.detailFormState(s)),i?.showModal()})}async loadTargetHistory(e){try{const i=await m(`/api/v1/targets/${e}/history?limit=720`);this.selected?.id===e&&(this.targetHistory=i)}catch(i){this.selected?.id===e&&(this.error=i instanceof Error?i.message:String(i))}finally{this.selected?.id===e&&(this.historyLoading=!1)}}closeDetailDialog(){this.renderRoot.querySelector("#detail-dialog")?.close(),this.detailDirty=!1,this.detailTab="details",this.detailInitialState="",this.selected=void 0,this.targetHistory=void 0,this.historyLoading=!1}showDialog(e){this.renderRoot.querySelector(`#${e}`)?.showModal()}dismissOnBackdrop(e){const i=e.currentTarget;e.target===i&&(i.close(),i.id==="detail-dialog"&&this.closeDetailDialog())}navigate(e,i){e.preventDefault(),this.activeSection=i,window.history.pushState(null,"",z[i]),this.renderRoot.querySelector(".account-menu")?.removeAttribute("open")}closeDialog(e){this.renderRoot.querySelector(`#${e}`)?.close()}selectDetailTab(e){this.detailTab=e}toggleMaxRedirects(e){const i=e.currentTarget,s=i.form?.elements.namedItem("max_redirects");s&&(s.disabled=!i.checked),i.form&&this.compareDetailForm(i.form)}detailFormState(e){return JSON.stringify([...new FormData(e).entries()])}compareDetailForm(e){this.detailDirty=this.detailFormState(e)!==this.detailInitialState}updateDetailDirty(e){this.compareDetailForm(e.currentTarget)}}v([f()],b.prototype,"targets");v([f()],b.prototype,"trashedTargets");v([f()],b.prototype,"channels");v([f()],b.prototype,"alerts");v([f()],b.prototype,"transitions");v([f()],b.prototype,"secrets");v([f()],b.prototype,"cluster");v([f()],b.prototype,"joinTokens");v([f()],b.prototype,"identities");v([f()],b.prototype,"apiTokens");v([f()],b.prototype,"session");v([f()],b.prototype,"authReady");v([f()],b.prototype,"newApiToken");v([f()],b.prototype,"editingIdentity");v([f()],b.prototype,"error");v([f()],b.prototype,"live");v([f()],b.prototype,"saving");v([f()],b.prototype,"selected");v([f()],b.prototype,"targetHistory");v([f()],b.prototype,"historyLoading");v([f()],b.prototype,"channelKind");v([f()],b.prototype,"editingChannel");v([f()],b.prototype,"channelTestMessage");v([f()],b.prototype,"testingChannel");v([f()],b.prototype,"joinCommand");v([f()],b.prototype,"alertSearch");v([f()],b.prototype,"alertDeliveryFilter");v([f()],b.prototype,"alertKindFilter");v([f()],b.prototype,"alertAcknowledgedFilter");v([f()],b.prototype,"search");v([f()],b.prototype,"statusFilter");v([f()],b.prototype,"sort");v([f()],b.prototype,"selectedIds");v([f()],b.prototype,"activeSection");v([f()],b.prototype,"copied");v([f()],b.prototype,"setupMode");v([f()],b.prototype,"setup");v([f()],b.prototype,"warningDismissed");v([f()],b.prototype,"unlimitedUses");v([f()],b.prototype,"theme");v([f()],b.prototype,"detailDirty");v([f()],b.prototype,"detailTab");class zs extends b{async createTarget(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i),a=i.querySelector("http-assertion-editor")?.value??[],n=Le(s,s.getAll("channel_id").map(String),s.get("use_default_channels")==="on",void 0,a);this.saving=!0;try{await m("/api/v1/targets",{method:"POST",body:JSON.stringify(n)}),i.reset(),this.closeTargetDialog(),await this.refresh()}catch(o){this.error=o instanceof Error?o.message:String(o)}finally{this.saving=!1}}async updateTarget(e){if(e.preventDefault(),!this.selected)return;const i=new FormData(e.currentTarget),s=e.currentTarget.querySelector("http-assertion-editor")?.value??[];let a=`/api/v1/nodes/${this.selected.id}`,n={name:String(i.get("name"))};if(this.selected.kind==="http"){const o=i.get("follow_redirects")==="on";a=`/api/v1/targets/${this.selected.id}`,n={name:String(i.get("name")),kind:"http",url:String(i.get("url")),method:String(i.get("method")),accepted_statuses:String(i.get("statuses")).split(",").map(r=>{const[l,d]=r.trim().split("-").map(Number);return{start:l,end:d||l}}),follow_redirects:o,max_redirects:o?Number(i.get("max_redirects")):0,interval_seconds:Number(i.get("interval")),timeout_seconds:Number(i.get("timeout")),failure_threshold:Number(i.get("failures")),locations:Number(i.get("locations")),headers:Object.fromEntries(Object.entries(this.selected.headers).map(([r,l])=>[r,l.kind==="literal"?l.value:{secret_id:l.secret_id}])),body:this.selected.body?.kind==="literal"?this.selected.body.value:this.selected.body?{secret_id:this.selected.body.secret_id}:null,assertions:s,skip_tls_verification:i.get("skip_tls_verification")==="on",tls_ca_secret_id:String(i.get("tls_ca_secret_id")??"")||null,tls_client_certificate_secret_id:String(i.get("tls_client_certificate_secret_id")??"")||null,tls_client_private_key_secret_id:String(i.get("tls_client_private_key_secret_id")??"")||null,notification_channel_ids:i.getAll("channel_id").map(String),use_default_channels:i.get("use_default_channels")==="on"}}this.selected.kind!=="http"&&this.selected.kind!=="node"&&(a=`/api/v1/targets/${this.selected.id}`,n=Le(i,i.getAll("channel_id").map(String),i.get("use_default_channels")==="on",this.selected.kind,s)),this.saving=!0;try{await m(a,{method:"PUT",body:JSON.stringify(n)}),this.closeDetailDialog(),await this.refresh()}catch(o){this.error=o instanceof Error?o.message:String(o)}finally{this.saving=!1}}async deleteTarget(){if(!(!this.selected||!window.confirm("Move this Target and its history to Trash? You can restore it before its retention period expires."))){this.saving=!0;try{await m(`/api/v1/targets/${this.selected.id}`,{method:"DELETE"}),this.closeDetailDialog(),await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async restoreTarget(e){window.confirm(`Restore ${e.name} with its settings and history?`)&&await this.saveResource(()=>m(`/api/v1/trash/targets/${e.id}/restore`,{method:"POST"}))}async purgeTarget(e){window.confirm(`Permanently delete ${e.name} and all of its history? This cannot be undone.`)&&await this.saveResource(()=>m(`/api/v1/trash/targets/${e.id}`,{method:"DELETE"}))}async setPaused(e){if(this.selected){this.saving=!0;try{await m(`/api/v1/targets/${this.selected.id}/${e?"pause":"resume"}`,{method:"POST"}),this.closeDetailDialog(),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async createSecret(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i);this.saving=!0;try{await m("/api/v1/secrets",{method:"POST",body:JSON.stringify({name:s.get("name"),value:s.get("value")})}),i.reset(),this.closeDialog("secret-dialog"),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}finally{this.saving=!1}}async createChannel(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i),a=this.editingChannel,n=Me(s,this.channelKind,a!==void 0);this.saving=!0;try{await m(a?`/api/v1/channels/${a.id}`:"/api/v1/channels",{method:a?"PUT":"POST",body:JSON.stringify(n)}),await this.refresh(),i.reset(),this.editingChannel=void 0,this.channelKind="webhook",this.channelTestMessage="",this.closeDialog("channel-dialog")}catch(o){this.error=o instanceof Error?o.message:String(o)}finally{this.saving=!1}}openChannelDialog(e){this.editingChannel=e,this.channelKind=e?.kind??"webhook",this.channelTestMessage="",this.showDialog("channel-dialog")}async setChannelDefault(e,i){try{await m(`/api/v1/channels/${e.id}/default`,{method:"PUT",body:JSON.stringify({default:i})}),await this.refresh()}catch(s){this.error=s instanceof Error?s.message:String(s)}}async testChannel(e){const i=e.currentTarget.form;if(!(!i||![...i.querySelectorAll("[data-test-required]")].every(a=>a.reportValidity()))){this.testingChannel=!0,this.channelTestMessage="";try{const a=Me(new FormData(i),this.channelKind);await m("/api/v1/channels/test",{method:"POST",body:JSON.stringify(a)}),this.channelTestMessage="Test sent"}catch(a){const n=a instanceof Error?a.message:String(a);this.channelTestMessage=`Test failed: ${n}`}finally{this.testingChannel=!1}}}openTokenDialog(){this.unlimitedUses=!1,this.showDialog("token-config-dialog")}async createJoinToken(e){e.preventDefault();const i=new FormData(e.currentTarget);this.saving=!0;try{const s=await m("/api/v1/join-tokens",{method:"POST",body:JSON.stringify({expires_in_seconds:Number(i.get("expiration_days"))*86400,max_uses:this.unlimitedUses?null:Number(i.get("max_uses"))})});this.joinCommand=`upgrid --join '${s.url}'`,this.copied=!1,await this.refresh(),this.closeDialog("token-config-dialog"),this.showDialog("join-dialog")}catch(s){this.error=s instanceof Error?s.message:String(s)}finally{this.saving=!1}}passwordsMatch(e){const i=e.elements.namedItem("password"),s=e.elements.namedItem("password_confirmation");return!i||!s?!0:(s.setCustomValidity(i.value===s.value?"":"Passwords do not match."),e.reportValidity())}async createIdentity(e){e.preventDefault();const i=e.currentTarget;if(!this.passwordsMatch(i))return;const s=new FormData(i);await this.saveResource(async()=>{await m("/api/v1/identities",{method:"POST",body:JSON.stringify({username:String(s.get("username")??""),password:String(s.get("password")??"")})}),i.reset(),this.closeDialog("add-user-dialog")})}async updateIdentity(e,i){i.preventDefault();const s=i.currentTarget;if(!this.passwordsMatch(s))return;const a=new FormData(s),n=String(a.get("password")??"");await this.saveResource(async()=>{await m(`/api/v1/identities/${e.id}`,{method:"PUT",body:JSON.stringify({username:String(a.get("username")??""),password:n||null})}),e.id===this.session?.identity_id&&n?await this.logout():(this.closeDialog("edit-user-dialog"),this.editingIdentity=void 0)})}async deleteIdentity(e){window.confirm(`Delete identity ${e.username}? Its API Tokens will also be revoked.`)&&await this.saveResource(()=>m(`/api/v1/identities/${e.id}`,{method:"DELETE"}))}async createApiToken(e){e.preventDefault();const i=e.currentTarget,s=new FormData(i);await this.saveResource(async()=>{const a=Number(s.get("expires_in_days")),n=await m("/api/v1/api-tokens",{method:"POST",body:JSON.stringify({name:String(s.get("name")??""),expires_in_seconds:a?a*86400:null})});this.newApiToken=n.value,i.reset(),this.closeDialog("api-token-dialog")})}async revokeApiToken(e){window.confirm(`Revoke API Token ${e.name}?`)&&await this.saveResource(()=>m(`/api/v1/api-tokens/${e.id}`,{method:"DELETE"}))}async setNodeDrain(e,i){await this.saveResource(()=>m(`/api/v1/nodes/${e.id}/drain`,{method:"PUT",body:JSON.stringify({draining:i,force:!1})}))}async removeNode(e,i){const s=i?`Replace failed Node ${e.name}? Confirm that it is permanently stopped. Its assignments will be released immediately.`:`Remove drained Node ${e.name} from the Cluster?`;window.confirm(s)&&(await this.saveResource(()=>m(`/api/v1/nodes/${e.id}?force=${i}`,{method:"DELETE"})),i&&!this.error&&this.openTokenDialog())}async acknowledgeAlert(e){await this.updateAlert("acknowledge",e)}async retryAlert(e){await this.updateAlert("retry",e)}async updateAlert(e,i){await this.saveResource(()=>m(`/api/v1/alerts/${e}`,{method:"POST",body:JSON.stringify({target_id:i.target_id,channel_id:i.channel_id,scheduled_at_ms:i.scheduled_at_ms,kind:i.kind})}))}async saveResource(e){this.saving=!0,this.error="";try{await e(),this.session&&await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async setupChanged(e){const i=e.detail;if(this.setup=i,this.setupMode=i.setup,window.history.replaceState(null,"",i.path),i.setup){i.cluster_ready&&(this.session=await m("/api/v1/auth/session"),await this.refresh(),this.connectEvents());return}this.activeSection="overview",await this.refresh(),this.connectEvents()}async revokeJoinToken(e){if(window.confirm("Revoke this Join Token? Nodes using it will no longer be admitted.")){this.saving=!0;try{await m(`/api/v1/join-tokens/${e.id}`,{method:"DELETE"}),await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}}async copyJoinCommand(){let e=!1;try{await navigator.clipboard.writeText(this.joinCommand),e=!0}catch{const i=Object.assign(document.createElement("textarea"),{value:this.joinCommand});i.style.cssText="position: fixed; opacity: 0",document.body.append(i),i.select(),e=document.execCommand("copy"),i.remove()}if(!e){this.error="Could not copy the Join command";return}this.copied=!0,window.setTimeout(()=>this.copied=!1,2e3)}toggleSelected(e,i){const s=new Set(this.selectedIds);i?s.add(e):s.delete(e),this.selectedIds=s}async bulkPause(e){this.saving=!0;try{await Promise.all([...this.selectedIds].map(i=>m(`/api/v1/targets/${i}/${e?"pause":"resume"}`,{method:"POST"}))),this.selectedIds=new Set,await this.refresh()}catch(i){this.error=i instanceof Error?i.message:String(i)}finally{this.saving=!1}}async bulkDelete(){if(window.confirm(`Move ${this.selectedIds.size} selected Targets and their history to Trash?`)){this.saving=!0;try{await Promise.all([...this.selectedIds].map(e=>m(`/api/v1/targets/${e}`,{method:"DELETE"}))),this.selectedIds=new Set,await this.refresh()}catch(e){this.error=e instanceof Error?e.message:String(e)}finally{this.saving=!1}}}async cleanupSecrets(){const e=this.secrets.filter(i=>!i.referenced);!e.length||!window.confirm(`Permanently delete ${e.length} unused ${e.length===1?"Secret":"Secrets"}? References are checked again when cleanup commits.`)||await this.saveResource(()=>m("/api/v1/secrets/unreferenced",{method:"DELETE"}))}async deleteResource(e,i,s){if(window.confirm(`Delete ${s}?`))try{await m(`/api/v1/${e}/${i}`,{method:"DELETE"}),await this.refresh()}catch(a){this.error=a instanceof Error?a.message:String(a)}}}const Hs={width:24,height:24,body:'<path fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M17 3a2.85 2.83 0 1 1 4 4L7.5 20.5L2 22l1.5-5.5Zm-2 2l4 4"/>'};function Js(t,e){const i=e.search.trim().toLocaleLowerCase();return(!i||`${t.target_name} ${t.channel_name}`.toLocaleLowerCase().includes(i))&&(e.delivery==="all"||t.delivery===e.delivery)&&(e.kind==="all"||t.kind===e.kind)&&(e.acknowledged==="all"||(e.acknowledged==="yes"?t.acknowledged_at_ms!==null:t.acknowledged_at_ms===null))}function Vs(t){return t.delivery==="pending"?t.next_attempt_at_ms===null?`${t.attempts} attempts`:`${t.attempts} attempts · next ${new Date(t.next_attempt_at_ms).toLocaleString()}`:t.delivery==="failed"?t.diagnostic??"Delivery failed":t.completed_at_ms===null?"Delivered":`Delivered ${new Date(t.completed_at_ms).toLocaleString()}`}function Bs(t,e,i,s,a,n){const o=t.filter(r=>Js(r,s));return c`
    <section class="heading" id="alerts">
      <div><span class="eyebrow">Delivery history</span><h1>Alerts</h1></div>
      <button class="button" @click=${n.create}>Add channel</button>
    </section>
    <section class="panel alert-history" aria-label="Alert history">
      <div class="panel-head"><h2>Notification deliveries</h2><span class="meta">${o.length} of ${t.length} alerts</span></div>
      <div class="alert-filters">
        <label>Search<input type="search" .value=${s.search} placeholder="Target or channel" @input=${r=>n.setSearch(r.target.value)} /></label>
        <label>Delivery<select .value=${s.delivery} @change=${r=>n.setDelivery(r.target.value)}><option value="all">All</option><option value="pending">Pending</option><option value="delivered">Delivered</option><option value="failed">Failed</option></select></label>
        <label>Transition<select .value=${s.kind} @change=${r=>n.setKind(r.target.value)}><option value="all">All</option><option value="down">Down</option><option value="recovered">Recovered</option></select></label>
        <label>Acknowledged<select .value=${s.acknowledged} @change=${r=>n.setAcknowledged(r.target.value)}><option value="all">All</option><option value="no">No</option><option value="yes">Yes</option></select></label>
      </div>
      ${o.length?o.map(r=>c`
                <div class="resource alert-resource">
                  <div class="alert-summary">
                    <div class="channel-title">
                      <strong>${r.target_name}</strong>
                      <span class=${`badge ${r.kind==="recovered"?"up":"down"}`}>${r.kind}</span>
                      <span class="badge">${r.delivery}</span>
                      ${r.acknowledged_at_ms===null?g:c`<span class="badge">acknowledged</span>`}
                    </div>
                    <code>${r.channel_name} · ${new Date(r.scheduled_at_ms).toLocaleString()}</code>
                    <span class="meta">${Vs(r)}</span>
                  </div>
                  <div class="alert-actions">
                    ${r.delivery==="failed"?c`<button class="button secondary" ?disabled=${a} @click=${()=>n.retry(r)}>Retry</button>`:g}
                    ${r.acknowledged_at_ms===null?c`<button class="button secondary" ?disabled=${a} @click=${()=>n.acknowledge(r)}>Acknowledge</button>`:g}
                  </div>
                </div>
              `):c`<div class="empty">No alerts match these filters.</div>`}
    </section>
    <div class="page-columns">
      <section class="panel" aria-label="Availability history">
        <div class="panel-head"><h2>Availability transitions</h2><span class="meta">${e.length} events</span></div>
        ${e.length?e.map(r=>{const l=r.kind==="recovered"?"up":"down";return c`
                <div class="resource">
                  <div class="transition-main">
                    <span class=${`state ${l}`} aria-hidden="true"></span>
                    <div>
                      <strong>${r.target_name}</strong>
                      <code>${new Date(r.scheduled_at_ms).toLocaleString()}</code>
                    </div>
                  </div>
                  <span class=${`badge ${l}`}>${r.kind}</span>
                </div>
              `}):c`<div class="empty">No availability transitions.</div>`}
      </section>
      <section class="panel" aria-label="Notification channels">
        <div class="panel-head"><h2>Notification channels</h2><span class="meta">${i.length} configured</span></div>
        ${i.length?i.map(r=>c`
              <div class="resource channel-resource">
                <div class="channel-summary"><div class="channel-title"><strong>${r.name}</strong><span class="badge">${r.kind}</span></div><code>${r.destination}</code></div>
                <div class="channel-actions">
                  <label class="switch"><span>Default</span><input class="switch-control" type="checkbox" role="switch" aria-label=${`Default channel ${r.name}`} .checked=${r.default} @change=${l=>n.setDefault(r,l.target.checked)} /></label>
                  <button class="button secondary icon-button" aria-label=${`Edit channel ${r.name}`} title=${`Edit ${r.name}`} @click=${()=>n.edit(r)}><iconify-icon .icon=${Hs} aria-hidden="true"></iconify-icon></button>
                  <button class="button danger icon-button" aria-label=${`Delete channel ${r.name}`} title=${`Delete ${r.name}`} @click=${()=>n.remove(r)}><iconify-icon .icon=${ie} aria-hidden="true"></iconify-icon></button>
                </div>
              </div>
            `):c`<div class="empty">No notification channels.</div>`}
      </section>
    </div>
  `}function Ks(t,e,i){return c`
    <main class="shell setup-shell">
      <header>
        <div class="brand"><img src="/favicon.svg" alt="" /><div><strong>UpGrid</strong><span>Distributed service monitoring</span></div></div>
      </header>
      <section class="panel auth-panel" aria-labelledby="login-title">
        <form class="choice" @submit=${i.login}>
          <div><span class="eyebrow">Cluster access</span><h1 id="login-title">Sign in</h1><p class="meta">Use a replicated Operator Identity.</p></div>
          ${e?c`<div class="notice" role="alert">${e}</div>`:g}
          <label>Username<input name="username" autocomplete="username" required autofocus /></label>
          <label>Password<input name="password" type="password" autocomplete="current-password" required /></label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${t}>${t?"Signing in…":"Sign in"}</button></div>
        </form>
      </section>
    </main>`}function Gs(t,e,i){return t?c`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Change Password</h1></div></div>
      <section class="panel auth-panel">
        <form class="choice" @submit=${s=>i.updateIdentity(t,s)}>
          <input name="username" type="hidden" .value=${t.username} />
          <label>Username<input .value=${t.username} autocomplete="username" disabled /></label>
          <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" required autofocus /></label>
          <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required @input=${s=>s.currentTarget.setCustomValidity("")} /></label>
          <div class="dialog-actions"><button class="button" type="submit" ?disabled=${e}>Change Password</button></div>
        </form>
      </section>
    </div>`:c`<div class="empty">Current identity unavailable.</div>`}function Ws(t,e,i,s,a){return c`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>Users</h1></div><button class="button" type="button" @click=${a.openAddUser}>Add User</button></div>
      <section class="panel" aria-label="Operator Identities">
        <div class="panel-head"><h2>Operator Identities</h2><span class="meta">${t.length} administrators</span></div>
        ${t.map(n=>c`
            <div class="resource user-resource">
              <button class="resource-main" type="button" aria-label=${`Edit user ${n.username}`} ?disabled=${s} @click=${()=>a.openEditUser(n)}>
                <span>
                  <strong>${n.username}</strong>
                  <code>Operator Identity${n.id===e?" · Current user":""}</code>
                </span>
              </button>
              <button class="button danger icon-button" type="button" aria-label=${`Delete user ${n.username}`} title=${`Delete ${n.username}`} ?disabled=${n.id===e||s} @click=${()=>a.deleteIdentity(n)}><iconify-icon .icon=${ie} aria-hidden="true"></iconify-icon></button>
            </div>`)}
      </section>
    </div>
    <dialog id="add-user-dialog" aria-labelledby="add-user-title" @click=${a.dismissDialog}>
      <div class="dialog-head"><h2 id="add-user-title">Add User</h2></div>
      <form @submit=${a.createIdentity}>
        <label>Username<input name="username" autocomplete="username" required autofocus /></label>
        <label>Password<input name="password" type="password" minlength="12" autocomplete="new-password" required /></label>
        <label>Confirm password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" required @input=${n=>n.currentTarget.setCustomValidity("")} /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${a.closeAddUser}>Cancel</button><button class="button" type="submit" ?disabled=${s}>${s?"Adding…":"Add User"}</button></div>
      </form>
    </dialog>
    ${i?c`
          <dialog id="edit-user-dialog" aria-labelledby="edit-user-title" @click=${a.dismissDialog}>
            <div class="dialog-head"><h2 id="edit-user-title">Edit User</h2></div>
            <form @submit=${n=>a.updateIdentity(i,n)}>
              <label>Username<input name="username" .value=${i.username} autocomplete="username" required autofocus /></label>
              <label>New password<input name="password" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" /></label>
              <label>Confirm new password<input name="password_confirmation" type="password" minlength="12" autocomplete="new-password" placeholder="Keep current password" @input=${n=>n.currentTarget.setCustomValidity("")} /></label>
              <div class="dialog-actions"><button class="button secondary" type="button" @click=${a.closeEditUser}>Cancel</button><button class="button" type="submit" ?disabled=${s}>Save changes</button></div>
            </form>
          </dialog>`:g}`}function Qs(t,e,i,s){return c`
    <div class="admin-page">
      <div class="heading"><div><span class="eyebrow">Administration</span><h1>API Tokens</h1></div><button class="button" type="button" @click=${s.openApiToken}>New token</button></div>
      <section class="panel" aria-label="API Tokens">
        <div class="panel-head"><h2>API Tokens</h2><span class="meta">${t.length} active</span></div>
        ${e?c`<div class="notice token-value" role="status"><strong>Copy this token now.</strong><code>${e}</code><button class="button secondary" @click=${s.dismissToken}>Dismiss</button></div>`:g}
        ${t.length?t.map(a=>c`<div class="resource"><div><strong>${a.name}</strong><code>${a.expires_at_ms?`Expires ${new Date(a.expires_at_ms).toLocaleString()}`:"Never expires"}</code></div><button class="button danger" @click=${()=>s.revokeApiToken(a)}>Revoke</button></div>`):c`<div class="empty">No API Tokens.</div>`}
      </section>
    </div>
    <dialog id="api-token-dialog" aria-labelledby="api-token-title" @click=${s.dismissDialog}>
      <div class="dialog-head"><h2 id="api-token-title">New API Token</h2></div>
      <form @submit=${s.createApiToken}>
        <label>Name<input name="name" placeholder="Automation" required autofocus /></label>
        <label>Expires in days<input name="expires_in_days" type="number" min="1" max="365" placeholder="Never" /></label>
        <div class="dialog-actions"><button class="button secondary" type="button" @click=${s.closeApiToken}>Cancel</button><button class="button" type="submit" ?disabled=${i}>${i?"Creating…":"Create API Token"}</button></div>
      </form>
    </dialog>`}const Ys={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4m0-4h.01"/></g>'},Zs=ve`
  .form-field { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
  .title-with-help { display: flex; align-items: center; gap: 3px; }
  .help-tooltip-wrap { position: relative; display: inline-flex; align-items: center; }
  .help-tooltip-trigger { display: grid; width: 28px; height: 28px; place-items: center; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 0; cursor: pointer; user-select: none; transition: background-color 160ms ease, color 160ms ease; }
  .help-tooltip-trigger:hover { background: var(--panel-2); color: var(--text); }
  .help-tooltip-trigger iconify-icon { width: 16px; height: 16px; font-size: 16px; }
  .help-tooltip { position: absolute; top: calc(100% + 6px); left: -60px; z-index: 10; width: 280px; max-width: calc(100vw - 64px); border: 1px solid var(--line); border-radius: 9px; background: var(--panel-2); color: var(--text); box-shadow: 0 10px 30px var(--dialog-shadow); padding: 9px 10px; font-size: 12px; font-weight: 400; line-height: 1.45; opacity: 0; visibility: hidden; transform: translateY(-3px); pointer-events: none; transition: opacity 140ms ease, transform 140ms ease, visibility 140ms; }
  .help-tooltip-wrap:hover .help-tooltip, .help-tooltip-wrap:focus-within .help-tooltip { opacity: 1; visibility: visible; transform: translateY(0); }
`;function H(t,e,i){return c`
    <span class="help-tooltip-wrap">
      <button class="help-tooltip-trigger" type="button" aria-label=${e} aria-describedby=${t}>
        <iconify-icon .icon=${Ys} aria-hidden="true"></iconify-icon>
      </button>
      <span class="help-tooltip" id=${t} role="tooltip">${i}</span>
    </span>
  `}function Xs(t,e){return t==="webhook"?c`<label
      >Webhook URL<input
        name="url"
        type="url"
        placeholder="https://hooks.example.com/upgrid"
        .value=${e?.destination??""}
        data-test-required
        required
    /></label>`:t==="telegram"?c`
      <label
        ><span class="title-with-help"
          >Bot token
          ${H("telegram-token-help","About Telegram bot token storage",e?"Leave this blank to keep the automatically managed Secret, or enter a replacement token.":"Creating the Channel encrypts this token as an automatically managed Secret. Test sends use the entered value without storing it.")}</span
        ><input
          name="bot_token"
          type="password"
          autocomplete="off"
          placeholder=${e?"Leave blank to keep current token":""}
          ?required=${e===void 0}
      /></label>
      <label
        >Chat ID<input name="chat_id" .value=${e?.destination??""} data-test-required required
      /></label>
    `:c`
    <label
      >SMTP host<input name="host" placeholder="smtp.example.com" .value=${e?.destination??""} required
    /></label>
    <div class="row">
      <label
        >Port<input
          name="port"
          type="number"
          min="1"
          max="65535"
          .value=${String(e?.port??587)}
          required
      /></label>
      <label
        >Security<select name="security" .value=${e?.security??"start_tls"}>
          <option value="start_tls">STARTTLS</option>
          <option value="tls">Implicit TLS</option>
          <option value="none">Plaintext</option>
        </select></label
      >
    </div>
    <label
      >Username<input name="username" autocomplete="username" .value=${e?.username??""}
    /></label>
    <div class="form-field">
      <div class="title-with-help">
        <label for="smtp-password">Password</label>
        ${H("smtp-password-help","About SMTP password storage",e?"Leave this blank to keep the automatically managed Secret. Clear the username to disable authentication.":"Enter a username and password together to enable authentication. The password is encrypted as an automatically managed Secret.")}
      </div>
      <input
        id="smtp-password"
        name="password"
        type="password"
        autocomplete="off"
        placeholder=${e?"Leave blank to keep current password":"Optional"}
      />
    </div>
    <label
      >From<input name="from" placeholder="UpGrid <upgrid@example.com>" .value=${e?.from??""} required
    /></label>
    <label
      >Recipient<input name="to" placeholder="on-call@example.com" .value=${e?.to??""} required
    /></label>
  `}const ea={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5c.08-1.25-.27-2.48-1-3.5c.28-1.15.28-2.35 0-3.5c0 0-1 0-3 1.5c-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5c-.39.49-.68 1.05-.85 1.65c-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/></g>'},ta={width:24,height:24,body:'<g fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2"><path d="M21.54 15H17a2 2 0 0 0-2 2v4.54M7 3.34V5a3 3 0 0 0 3 3v0a2 2 0 0 1 2 2v0c0 1.1.9 2 2 2v0a2 2 0 0 0 2-2v0c0-1.1.9-2 2-2h3.17M11 21.95V18a2 2 0 0 0-2-2v0a2 2 0 0 1-2-2v-1a2 2 0 0 0-2-2H2.05"/><circle cx="12" cy="12" r="10"/></g>'};function Pe(){return c`
    <footer aria-label="Project information">
      <div class="footer-links">
        <a href="https://miao.dev">A Project by Pop</a>
        <span aria-hidden="true">|</span>
        <a href="https://github.com/George-Miao/UpGrid">
          <iconify-icon .icon=${ea} aria-hidden="true"></iconify-icon>GitHub
        </a>
        <span aria-hidden="true">|</span>
        <a href="https://upgrid.rs">
          <iconify-icon .icon=${ta} aria-hidden="true"></iconify-icon>upgrid.rs
        </a>
      </div>
      <div class="footer-powered">
        Proudly powered by <a href="https://compio.rs/">Compio</a> and
        <a href="https://github.com/databendlabs/openraft">OpenRaft</a>
      </div>
    </footer>
  `}function ei(t,e=[],i=!0){return c`
    <div class="channel-fields">
      <label class="switch">
        <span>Use default channels</span>
        <input
          class="switch-control"
          name="use_default_channels"
          type="checkbox"
          role="switch"
          .checked=${i}
          @change=${a=>{const n=a.currentTarget;n.closest(".channel-fields")?.querySelectorAll('input[data-default="true"]').forEach(r=>{r.disabled=n.checked,r.checked=n.checked||r.dataset.explicit==="true"}),n.form?.dispatchEvent(new Event("input",{bubbles:!0}))}}
        />
      </label>
      <div class="channel-options">
        ${t.length?t.map(a=>{const n=e.includes(a.id),o=i&&a.default;return c`
                  <label class="checkbox-option">
                    <span class="switch-label">${a.name} <span class="badge">${a.kind}</span></span>
                    <input
                      class="checkbox-control"
                      name="channel_id"
                      type="checkbox"
                      value=${a.id}
                      data-default=${String(a.default)}
                      data-explicit=${String(n)}
                      .checked=${n||o}
                      ?disabled=${o}
                      @change=${r=>{const l=r.currentTarget;l.dataset.explicit=String(l.checked)}}
                    />
                  </label>
                `}):c`<p class="meta">No notification channels are available.</p>`}
      </div>
    </div>`}function ia(t,e=null,i=null,s=null){const a=n=>c`
    <option value="">Not configured</option>
    ${t.map(o=>c`<option value=${o.id} ?selected=${o.id===n}>${o.name}</option>`)}
  `;return c`
    <fieldset class="tls-fields">
      <legend>HTTPS trust and mutual TLS</legend>
      <label>Custom CA bundle Secret<select name="tls_ca_secret_id">${a(e)}</select></label>
      <div class="row">
        <label>Client certificate Secret<select name="tls_client_certificate_secret_id">${a(i)}</select></label>
        <label>Client private key Secret<select name="tls_client_private_key_secret_id">${a(s)}</select></label>
      </div>
      <p class="meta">PEM values stay encrypted. Client certificate and private key must be configured together.</p>
    </fieldset>
  `}const ti={http:"https://example.com/health",tcp:"database.internal:5432",dns:"service.internal",icmp:"192.0.2.10",tls:"example.com:443"};function Ye(t,e){t.querySelectorAll("[role='tab']").forEach(i=>{const s=i.dataset.tab===e;i.setAttribute("aria-selected",String(s)),i.tabIndex=s?0:-1}),t.querySelectorAll("[role='tabpanel']").forEach(i=>{i.hidden=i.dataset.panel!==e})}function ii(t,e){const i=t.elements.namedItem("url");i&&(i.placeholder=ti[e],i.type=e==="http"?"url":"text"),t.querySelectorAll("[data-http-only]").forEach(o=>{o.hidden=e!=="http"});const s=t.querySelector("[role='tab'][aria-selected='true']")?.dataset.tab??"general",a=t.querySelector("[data-tab='assertions']");a&&(a.disabled=e!=="http"),Ye(t,e!=="http"&&s==="assertions"?"general":s);const n=t.elements.namedItem("method");n&&(n.disabled=e!=="http",n.disabled&&(n.value="GET"))}function sa(t){const e=t.currentTarget;e.form&&ii(e.form,e.value)}function le(t){const e=t.currentTarget;e.form&&e.dataset.tab&&Ye(e.form,e.dataset.tab)}function aa(t){const e=t.currentTarget;queueMicrotask(()=>{ii(e,"http"),Ye(e,"general")})}function na(t,e,i){return c`
    <dialog id="target-dialog" aria-labelledby="add-target-title" @click=${i.backdrop}>
      <div class="dialog-head target-dialog-head">
        <h2 id="add-target-title">Add target</h2>
        <div class="form-tabs" role="tablist" aria-label="Target settings">
          <button id="target-general-tab" form="target-form" type="button" role="tab" data-tab="general" aria-controls="target-general-panel" aria-selected="true" @click=${le}>General</button>
          <button id="target-assertions-tab" form="target-form" type="button" role="tab" data-tab="assertions" aria-controls="target-assertions-panel" aria-selected="false" tabindex="-1" @click=${le}>Assertions</button>
          <button id="target-evaluation-tab" form="target-form" type="button" role="tab" data-tab="evaluation" aria-controls="target-evaluation-panel" aria-selected="false" tabindex="-1" @click=${le}>Evaluation</button>
          <button id="target-notifications-tab" form="target-form" type="button" role="tab" data-tab="notifications" aria-controls="target-notifications-panel" aria-selected="false" tabindex="-1" @click=${le}>Notifications</button>
        </div>
      </div>
      <form id="target-form" @submit=${i.create} @reset=${aa}>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" data-panel="general" aria-labelledby="target-general-tab">
          <label>Name<input name="name" placeholder="Production API" required autofocus /></label>
          <div class="row">
            <label>Type<select name="kind" @change=${sa}><option value="http">HTTP</option><option value="tcp">TCP connect</option><option value="dns">DNS resolution</option><option value="icmp">ICMP echo</option><option value="tls">TLS certificate</option></select></label>
            <label>URL / endpoint<input name="url" type="url" placeholder=${ti.http} required /></label>
          </div>
          <label data-http-only>Method<input name="method" value="GET" required /></label>
        </section>
        <section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" data-panel="assertions" data-http-only aria-labelledby="target-assertions-tab" hidden>
          <http-assertion-editor name="assertions" target-id="new"></http-assertion-editor>
        </section>
        <section id="target-evaluation-panel" class="target-tab-panel" role="tabpanel" data-panel="evaluation" aria-labelledby="target-evaluation-tab" hidden>
          <div class="row">
            <label>Interval (seconds)<input name="interval" type="number" min="1" value="60" required /></label>
            <label>Timeout (seconds)<input name="timeout" type="number" min="1" value="10" required /></label>
          </div>
          <div class="row">
            <label>Failures before Down<input name="failures" type="number" min="1" value="3" required /></label>
            <label>Evaluation locations<input name="locations" type="number" min="1" max="32" value="1" required /></label>
          </div>
        </section>
        <section id="target-notifications-panel" class="target-tab-panel" role="tabpanel" data-panel="notifications" aria-labelledby="target-notifications-tab" hidden>
          ${ei(t)}
        </section>
        <div class="dialog-actions">
          <button class="button secondary" type="button" @click=${i.close}>Cancel</button>
          <button class="button" type="submit" ?disabled=${e}>${e?"Creating…":"Create target"}</button>
        </div>
      </form>
    </dialog>`}function ra(t,e,i,s,a,n,o,r,l,d){const p=t.kind==="node",u=t.kind==="http",y=t.accepted_statuses.map(h=>h.start===h.end?h.start:`${h.start}-${h.end}`).join(","),x=t.history.slice(0,30).reverse(),k=Math.max(1,...x.map(h=>h.latency_ms)),C=e?.items??[],$=C.reduce((h,P)=>h+P.samples,0),K=C.reduce((h,P)=>h+P.successes,0),E=C.reduce((h,P)=>h+P.latency_total_ms,0),S=$?`${(K/$*100).toFixed(2)}%`:"—",_=new Map(o.map(h=>[h.id,h.name])),q=h=>new Date(h).toLocaleString(void 0,{month:"short",day:"numeric",hour:"2-digit",minute:"2-digit"}),w=h=>h>=1e3?`${(h/1e3).toFixed(h>=1e4?0:1)} s`:`${Math.round(h)} ms`,T=$?w(E/$):"—",I=[{id:"details",label:"Details"},{id:"general",label:"General"},...u?[{id:"assertions",label:"Assertions"}]:[],...p?[]:[{id:"evaluation",label:"Evaluation"},{id:"notifications",label:"Notifications"}]],A=I.some(({id:h})=>h===n)?n:"details";return c`
    <dialog id="detail-dialog" aria-labelledby="target-detail-title" @click=${d.backdrop}>
      <div class="dialog-head target-dialog-head">
        <h2 id="target-detail-title">${p?"Node details":"Target details"}</h2>
        <div class="form-tabs" role="tablist" aria-label=${`${p?"Node":"Target"} details`}>
          ${I.map(({id:h,label:P})=>c`<button form="detail-form" type="button" role="tab" aria-controls=${`target-${h}-panel`} aria-selected=${String(A===h)} tabindex=${A===h?"0":"-1"} @click=${()=>d.selectTab(h)}>${P}</button>`)}
        </div>
        <button class="button secondary icon-button dialog-close" type="button" aria-label=${`Close ${p?"Node":"Target"} details`} title="Close" @click=${d.close}><iconify-icon .icon=${jt} aria-hidden="true"></iconify-icon></button>
      </div>
      <form id="detail-form" class="detail-form" @submit=${d.update} @input=${d.changed}>
        <section id="target-details-panel" class="target-tab-panel details-panel" role="tabpanel" aria-label="Details" ?hidden=${A!=="details"}>
          <section class="history">
            <div class="history-head"><h3>Long-term summary</h3><span class="meta">Last 30 days</span></div>
            ${i?c`<p class="meta">Loading long-term history…</p>`:$?c`
                    <div class="history-summary" aria-label="Long-term evaluation summary">
                      <div><span>Availability</span><strong>${S}</strong></div>
                      <div><span>Average latency</span><strong>${T}</strong></div>
                      <div><span>Evaluations</span><strong>${$.toLocaleString()}</strong></div>
                    </div>
                  `:c`<p class="meta">No long-term history recorded yet.</p>`}
          </section>
          <section class="history">
            <div class="history-head"><h3>Evaluation history</h3>${x.length?c`<span class="meta">Latest ${x.length}</span>`:g}</div>
            ${x.length?c`
              <div class="chart-plot">
                <div class="chart-scale" aria-hidden="true"><span>${w(k)}</span><span>${w(k/2)}</span><span>0 ms</span></div>
                <div class="history-chart" role="list" aria-label=${`Recent evaluation latency, 0 to ${w(k)}`}>
                  ${x.map(h=>{const P=h.succeeded?"Passed":"Failed",si=p||!u?h.succeeded?"reachable":"unreachable":h.status_code===null?"network error":`HTTP ${h.status_code}`,ai=_.get(h.executor_node_id)??`Node ${h.executor_node_id.slice(0,8)}`,Ze=`${P} at ${new Date(h.recorded_at_ms).toLocaleString()}: ${h.latency_ms} ms, ${si}. Executed by ${ai}`;return c`<span class="history-bar ${h.succeeded?"up":"down"}" role="listitem" aria-label=${Ze} title=${Ze} style=${`height: ${Math.max(8,h.latency_ms/k*100)}%`}></span>`})}
                </div>
              </div>
              <div class="chart-axis"><span>${q(x[0].recorded_at_ms)}</span><span>${q(x[x.length-1].recorded_at_ms)}</span></div>
              <div class="chart-legend"><span><i class="up"></i>Passed</span><span><i class="down"></i>Failed</span><span>Height = latency</span></div>
            `:c`<p class="meta">No evaluations recorded yet.</p>`}
          </section>
        </section>
        <section id="target-general-panel" class="target-tab-panel" role="tabpanel" aria-label="General" ?hidden=${A!=="general"}>
          <label>Name<input name="name" .value=${t.name} required /></label>
          ${p?c`<label>RPC URL<input .value=${t.url} disabled /></label>`:c`
                <div class="row"><label>Type<input .value=${t.kind.toUpperCase()} disabled /></label><label>URL / endpoint<input name="url" .value=${t.url} required /></label></div>
                ${u?c`
                      <div class="row"><label>Method<input name="method" .value=${t.method} required /></label><label>Expected statuses<input name="statuses" .value=${y} required /></label></div>
                      <div class="row"><label class="switch"><span>Follow redirects</span><input class="switch-control" name="follow_redirects" type="checkbox" role="switch" .checked=${t.follow_redirects} @change=${d.redirects} /></label><label>Maximum redirects<input name="max_redirects" type="number" min="0" .value=${String(t.max_redirects)} ?disabled=${!t.follow_redirects} required /></label></div>
                      <label class="switch"><span>Skip TLS verification</span><input class="switch-control" name="skip_tls_verification" type="checkbox" role="switch" .checked=${t.skip_tls_verification} /></label>
                      ${ia(l,t.tls_ca_secret_id,t.tls_client_certificate_secret_id,t.tls_client_private_key_secret_id)}
                    `:g}
              `}
        </section>
        ${u?c`<section id="target-assertions-panel" class="target-tab-panel" role="tabpanel" aria-label="Assertions" ?hidden=${A!=="assertions"}>
                <http-assertion-editor name="assertions" target-id=${t.id} .assertions=${t.assertions}></http-assertion-editor>
              </section>`:g}
        ${p?g:c`
              <section id="target-evaluation-panel" class="target-tab-panel" role="tabpanel" aria-label="Evaluation" ?hidden=${A!=="evaluation"}>
                <div class="row"><label>Interval (seconds)<input name="interval" type="number" min="1" .value=${String(t.interval_seconds)} required /></label><label>Timeout (seconds)<input name="timeout" type="number" min="1" .value=${String(t.timeout_seconds)} required /></label></div>
                <div class="row"><label>Failures before Down<input name="failures" type="number" min="1" .value=${String(t.failure_threshold)} required /></label><label>Evaluation locations<input name="locations" type="number" min="1" max="32" .value=${String(t.locations)} required /></label></div>
              </section>
              <section id="target-notifications-panel" class="target-tab-panel" role="tabpanel" aria-label="Notifications" ?hidden=${A!=="notifications"}>
                ${ei(r,t.notification_channel_ids,t.use_default_channels)}
              </section>
            `}
        ${A==="details"?p?g:c`<div class="dialog-actions"><div class="danger-actions">
                  <button class="button danger icon-button" type="button" aria-label="Move Target to Trash" title="Move to Trash" @click=${d.delete}><iconify-icon .icon=${ie} aria-hidden="true"></iconify-icon></button>
                  <button class=${`button ${t.paused?"success":"warning"} icon-button`} type="button" aria-label=${t.paused?"Resume evaluations":"Pause evaluations"} title=${t.paused?"Resume evaluations":"Pause evaluations"} @click=${()=>d.pause(!t.paused)}><iconify-icon .icon=${t.paused?Nt:Ot} aria-hidden="true"></iconify-icon></button>
                </div></div>`:c`<div class="dialog-actions"><button class="button" type="submit" aria-busy=${s?"true":"false"} ?disabled=${s||!a}>Save changes</button></div>`}
      </form>
    </dialog>`}var oa=Object.getOwnPropertyDescriptor,la=(t,e,i,s)=>{for(var a=s>1?void 0:s?oa(e,i):e,n=t.length-1,o;n>=0;n--)(o=t[n])&&(a=o(a)||a);return a};let Ue=class extends zs{render(){const t=this.targets.filter(r=>r.availability==="up").length,e=this.targets.filter(r=>r.availability==="down").length,i=this.alerts.filter(r=>r.delivery==="pending").length,s=qs(this.targets,this.live),a=["overview","alerts","cluster","trash"],n=this.targets.filter(r=>`${r.name} ${r.url}`.toLowerCase().includes(this.search.toLowerCase())).filter(r=>this.statusFilter==="all"?!0:this.statusFilter==="paused"?r.paused:r.availability===this.statusFilter).sort((r,l)=>this.sort==="status"&&r.availability.localeCompare(l.availability)||r.name.localeCompare(l.name)),o={login:r=>{this.login(r)},logout:()=>{this.logout()},createIdentity:r=>{this.createIdentity(r)},openAddUser:()=>this.showDialog("add-user-dialog"),closeAddUser:()=>this.closeDialog("add-user-dialog"),openEditUser:r=>{this.editingIdentity=r,this.updateComplete.then(()=>this.showDialog("edit-user-dialog"))},closeEditUser:()=>{this.closeDialog("edit-user-dialog"),this.editingIdentity=void 0},openApiToken:()=>this.showDialog("api-token-dialog"),closeApiToken:()=>this.closeDialog("api-token-dialog"),dismissDialog:r=>this.dismissOnBackdrop(r),updateIdentity:(r,l)=>{this.updateIdentity(r,l)},deleteIdentity:r=>{this.deleteIdentity(r)},createApiToken:r=>{this.createApiToken(r)},revokeApiToken:r=>{this.revokeApiToken(r)},dismissToken:()=>this.newApiToken=""};return this.authReady&&!this.setupMode&&!this.session?c`${Ks(this.saving,this.error,o)}${Pe()}`:this.setupMode&&this.setup?c`
        <main class="shell setup-shell">
          <header>
            <div class="brand">
              <img src="/favicon.svg" alt="" />
              <div><div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${this.live?"up":""}"></i>${this.live?"ready":"connecting"}</div></div><span>Distributed service monitoring</span></div>
            </div>
            <div></div>
            <div class="actions"><button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${St[this.theme]} aria-hidden="true"></iconify-icon></button></div>
          </header>
          ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:g}
          <upgrid-setup .setup=${this.setup} @setup-changed=${this.setupChanged}></upgrid-setup>
        </main>${Pe()}`:c`
      <main class="shell">
        <header>
          <div class="brand">
            <img src="/favicon.svg" alt="" />
            <div>
              <div class="brand-line"><strong>UpGrid</strong><div class="live"><i class="dot ${s.tone}"></i>${s.label}</div></div>
              <span>Distributed service monitoring</span>
            </div>
          </div>
          <nav aria-label="Primary">
            ${a.map(r=>c`<a class=${this.activeSection===r?"active":""} href=${z[r]} @click=${l=>this.navigate(l,r)}>${r[0].toUpperCase()}${r.slice(1)}</a>`)}
          </nav>
          <div class="actions">
            <button class="button secondary icon-button" aria-label=${`Theme: ${this.theme[0].toUpperCase()}${this.theme.slice(1)}`} title=${`Theme: ${this.theme}. Click to switch.`} @click=${this.cycleTheme}><iconify-icon .icon=${St[this.theme]} aria-hidden="true"></iconify-icon></button>
            <details class="account-menu">
              <summary class="button secondary icon-button" aria-label=${`Account menu for ${this.session?.username}`} title=${`Account: ${this.session?.username}`}><iconify-icon .icon=${Ei} aria-hidden="true"></iconify-icon></summary>
              <div class="account-dropdown" role="menu">
                <a class="button secondary" role="menuitem" href=${z.changePassword} @click=${r=>this.navigate(r,"changePassword")}>Change Password</a>
                <a class="button secondary" role="menuitem" href=${z.users} @click=${r=>this.navigate(r,"users")}>Manage user</a>
                <a class="button secondary" role="menuitem" href=${z.apiTokens} @click=${r=>this.navigate(r,"apiTokens")}>API Token</a>
                <div class="account-separator" role="separator"></div>
                <button class="button danger" role="menuitem" type="button" @click=${()=>{this.logout()}}>Logout</button>
              </div>
            </details>
          </div>
        </header>
        ${this.error?c`<div class="notice" role="alert">${this.error}</div>`:g}
        ${this.setup?.warning&&!this.warningDismissed?c`<div class="notice" role="status">${this.setup.warning}<button class="button secondary" style="float: right; margin: -6px" @click=${this.dismissWarning}>Dismiss</button></div>`:g}
        ${this.activeSection==="overview"?this.renderOverview(n,t,e,i):this.activeSection==="alerts"?Bs(this.alerts,this.transitions,this.channels,{search:this.alertSearch,delivery:this.alertDeliveryFilter,kind:this.alertKindFilter,acknowledged:this.alertAcknowledgedFilter},this.saving,{create:()=>this.openChannelDialog(),edit:r=>this.openChannelDialog(r),remove:r=>{this.deleteResource("channels",r.id,r.name)},setDefault:(r,l)=>{this.setChannelDefault(r,l)},acknowledge:r=>{this.acknowledgeAlert(r)},retry:r=>{this.retryAlert(r)},setSearch:r=>this.alertSearch=r,setDelivery:r=>this.alertDeliveryFilter=r,setKind:r=>this.alertKindFilter=r,setAcknowledged:r=>this.alertAcknowledgedFilter=r}):this.activeSection==="cluster"?this.renderClusterPage():this.activeSection==="trash"?this.renderTrashPage():this.activeSection==="changePassword"?Gs(this.identities.find(r=>r.id===this.session?.identity_id),this.saving,o):this.activeSection==="users"?Ws(this.identities,this.session?.identity_id,this.editingIdentity,this.saving,o):Qs(this.apiTokens,this.newApiToken,this.saving,o)}
      </main>${Pe()}
      ${na(this.channels,this.saving,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeTargetDialog(),create:r=>{this.createTarget(r)}})}
      ${this.selected?ra(this.selected,this.targetHistory,this.historyLoading,this.saving,this.detailDirty,this.detailTab,this.cluster?.members??[],this.channels,this.secrets,{backdrop:r=>this.dismissOnBackdrop(r),close:()=>this.closeDetailDialog(),update:r=>{this.updateTarget(r)},changed:r=>this.updateDetailDirty(r),redirects:r=>this.toggleMaxRedirects(r),delete:()=>{this.deleteTarget()},selectTab:r=>this.selectDetailTab(r),pause:r=>{this.setPaused(r)}}):g}
      <dialog id="secret-dialog" aria-labelledby="secret-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><div class="title-with-help"><h2 id="secret-title">Add secret</h2>${H("add-secret-help","About adding a Secret","Create an encrypted, write-only value to reference from Target requests or webhook headers through the HTTP API.")}</div></div>
        <form @submit=${this.createSecret}>
          <label>Name<input name="name" placeholder="Webhook token" required autofocus /></label>
          <label>Value<input name="value" type="password" autocomplete="new-password" required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("secret-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>Create secret</button></div>
        </form>
      </dialog>
      <dialog id="channel-dialog" aria-labelledby="channel-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="channel-title">${this.editingChannel?"Edit channel":"Add channel"}</h2><p>${this.editingChannel?"Update this destination without changing its Channel type.":"Send transitions through Telegram, SMTP, or a generic webhook."}</p></div>
        <form @submit=${this.createChannel}>
          <label>Type<select name="type" .value=${this.channelKind} ?disabled=${this.editingChannel!==void 0} @change=${r=>{this.channelKind=r.target.value,this.channelTestMessage=""}}><option value="webhook">Webhook</option><option value="telegram">Telegram</option><option value="smtp">SMTP email</option></select></label>
          <label>Name<input name="name" placeholder="On-call" .value=${this.editingChannel?.name??""} required /></label>
          ${Xs(this.channelKind,this.editingChannel)}
          <label class="switch"><span>Default channel</span><input class="switch-control" name="default" type="checkbox" role="switch" .checked=${this.editingChannel?.default??!1} /></label>
          <div class="dialog-actions">${this.channelTestMessage?c`<span class="meta" role="status" style="margin-right:auto">${this.channelTestMessage}</span>`:g}<button class="button secondary" type="button" @click=${()=>{this.editingChannel=void 0,this.closeDialog("channel-dialog")}}>Cancel</button>${this.editingChannel?g:c`<button class="button secondary" type="button" aria-busy=${this.testingChannel} ?disabled=${this.testingChannel||this.saving} @click=${this.testChannel}>${this.testingChannel?"Sending…":"Send test"}</button>`}<button class="button" type="submit" ?disabled=${this.saving||this.testingChannel}>${this.editingChannel?"Save changes":"Create channel"}</button></div>
        </form>
      </dialog>
      <dialog id="token-config-dialog" aria-labelledby="token-config-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="token-config-title">Create Join Token</h2><p>Choose how many days the token remains valid and whether it can be reused.</p></div>
        <form @submit=${this.createJoinToken}>
          <label>Expiration (days)<input name="expiration_days" type="number" min="1" step="1" value="1" required /></label>
          <label class="switch"><span>Unlimited uses</span><input class="switch-control" type="checkbox" role="switch" .checked=${this.unlimitedUses} @change=${r=>this.unlimitedUses=r.target.checked} /></label>
          <label>Maximum uses<input name="max_uses" type="number" min="1" step="1" value="1" ?disabled=${this.unlimitedUses} required /></label>
          <div class="dialog-actions"><button class="button secondary" type="button" @click=${()=>this.closeDialog("token-config-dialog")}>Cancel</button><button class="button" type="submit" ?disabled=${this.saving}>${this.saving?"Creating…":"Create token"}</button></div>
        </form>
      </dialog>
      <dialog id="join-dialog" aria-labelledby="join-title" @click=${this.dismissOnBackdrop}>
        <div class="dialog-head"><h2 id="join-title">Join Token Created</h2><p>This command contains Cluster credentials. Revoke the token when no longer needed.</p></div>
        <div class="join-command">${this.joinCommand}</div>
        <div class="dialog-actions" style="padding: 0 22px 22px"><button class="button secondary" @click=${()=>this.closeDialog("join-dialog")}>Close</button><button class="button" @click=${this.copyJoinCommand}>${this.copied?"Copied":"Copy command"}</button></div>
      </dialog>
    `}renderOverview(t,e,i,s){const a=this.targets.filter(l=>this.selectedIds.has(l.id)),n=a.some(l=>!l.paused),o=a.some(l=>l.paused),r=this.secrets.filter(l=>!l.referenced);return c`
      <section class="heading" id="overview">
        <div><span class="eyebrow">Cluster status</span><h1>Overview</h1></div>
        <button class="button" @click=${this.openTargetDialog}>Add target</button>
      </section>
      <section class="overview-top">
        <section class="summary" aria-label="Target summary">
          <div class="metric"><span>Targets</span><strong>${this.targets.length}</strong></div>
          <div class="metric"><span>Pending alerts</span><strong>${s}</strong></div>
          <div class="metric"><span>Up</span><strong>${e}</strong></div>
          <div class=${`metric down ${i?"active":""}`}><span>Down</span><strong>${i}</strong></div>
        </section>
        <section class="panel" aria-label="Secrets">
          <div class="panel-head"><div class="title-with-help"><h2>Secrets</h2>${H("secrets-help","About reusable Secrets","Reusable Secrets are encrypted and write-only. Reference them from Target headers or bodies and webhook headers or other Notification Channel credentials. UpGrid reports whether each Secret is referenced by an active or trashed Target or a Notification Channel.")}</div><div class="actions">${r.length?c`<button class="button danger" ?disabled=${this.saving} @click=${()=>this.cleanupSecrets()}>Delete unused (${r.length})</button>`:g}<button class="button secondary" @click=${()=>this.showDialog("secret-dialog")}>Add secret</button></div></div>
          ${this.secrets.length?this.secrets.map(l=>c`<div class="resource"><div><strong>${l.name}</strong><code>${l.id} · ${l.referenced?"In use":"Unused"}</code></div><button class="button danger icon-button" aria-label=${`Delete secret ${l.name}`} title=${`Delete ${l.name}`} @click=${()=>this.deleteResource("secrets",l.id,l.name)}><iconify-icon .icon=${ie} aria-hidden="true"></iconify-icon></button></div>`):c`<div class="empty">No reusable Secrets.</div>`}
        </section>
      </section>
      <section class="panel" aria-label="Targets">
        <div class="panel-head"><h2>Targets</h2><span class="meta">${this.targets.length} configured</span></div>
        <div class="toolbar">
          <input aria-label="Search targets" type="search" placeholder="Search name or URL" .value=${this.search} @input=${l=>this.search=l.target.value} />
          <select aria-label="Filter targets" .value=${this.statusFilter} @change=${l=>this.statusFilter=l.target.value}><option value="all">All states</option><option value="up">Up</option><option value="down">Down</option><option value="unknown">Unknown</option><option value="paused">Paused</option></select>
          <select aria-label="Sort targets" .value=${this.sort} @change=${l=>this.sort=l.target.value}><option value="name">Sort by name</option><option value="status">Sort by status</option></select>
        </div>
        ${this.selectedIds.size?c`<div class="bulk"><span class="meta">${this.selectedIds.size} selected</span><div class="bulk-actions"><button class="button secondary icon-button" aria-label="Unselect all" title="Unselect all" @click=${()=>this.selectedIds=new Set}><iconify-icon .icon=${jt} aria-hidden="true"></iconify-icon></button>${n?c`<button class="button warning icon-button" aria-label="Pause selected" title="Pause selected" @click=${()=>this.bulkPause(!0)}><iconify-icon .icon=${Ot} aria-hidden="true"></iconify-icon></button>`:g}${o?c`<button class="button success icon-button" aria-label="Resume selected" title="Resume selected" @click=${()=>this.bulkPause(!1)}><iconify-icon .icon=${Nt} aria-hidden="true"></iconify-icon></button>`:g}<button class="button danger icon-button" aria-label="Delete selected" title="Delete selected" @click=${this.bulkDelete}><iconify-icon .icon=${ie} aria-hidden="true"></iconify-icon></button></div></div>`:g}
        ${t.length?t.map(l=>this.renderTarget(l)):c`<div class="empty">${this.targets.length?"No Targets match these filters.":"No targets yet. Add the first one to begin monitoring."}</div>`}
      </section>
    `}renderTrashPage(){return c`
      <section class="heading" id="trash">
        <div><span class="eyebrow">Recover deleted monitors</span><h1>Trash</h1></div>
      </section>
      <section class="panel" aria-label="Trashed Targets">
        <div class="panel-head"><div class="title-with-help"><h2>Deleted Targets</h2>${H("trash-retention-help","About deleted Target retention","Settings and history remain recoverable until the retention deadline.")}</div><span class="meta">${this.trashedTargets.length} stored</span></div>
        ${this.trashedTargets.length?this.trashedTargets.map(t=>this.renderTrashedTarget(t)):c`<div class="empty">Trash is empty.</div>`}
      </section>
    `}renderTrashedTarget(t){return c`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.kind.toUpperCase()} · deleted ${new Date(t.deleted_at_ms).toLocaleString()} · permanently deleted ${new Date(t.purge_at_ms).toLocaleString()}</code>
        </div>
        <div class="actions">
          <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.restoreTarget(t)}>Restore</button>
          <button class="button danger" ?disabled=${this.saving} @click=${()=>this.purgeTarget(t)}>Delete permanently</button>
        </div>
      </div>
    `}renderClusterMember(t){return c`
      <div class="resource">
        <div>
          <strong>${t.name}</strong>
          <code>${t.raft_url} · ${t.active_assignments} active assignments</code>
        </div>
        <div class="actions">
          ${t.local?c`<span class="badge">This node</span>`:g}
          ${t.leader?c`<span class="badge">Leader</span>`:g}
          ${t.draining?c`<span class="badge">Draining</span>`:g}
          ${t.local?g:c`
                <button class="button secondary" ?disabled=${this.saving} @click=${()=>this.setNodeDrain(t,!t.draining)}>${t.draining?"Cancel drain":"Drain"}</button>
                ${t.draining&&t.active_assignments===0?c`<button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!1)}>Remove</button>`:g}
                <button class="button danger" ?disabled=${this.saving} @click=${()=>this.removeNode(t,!0)}>Replace failed</button>
              `}
        </div>
      </div>
    `}renderClusterPage(){return c`
      <section class="heading" id="cluster">
        <div><span class="eyebrow">Raft membership</span><h1>Cluster</h1></div>
        <div class="actions">
          <button class="button" @click=${this.openTokenDialog}>Create token</button>
        </div>
      </section>
      <div class="page-columns">
      <section class="panel" aria-label="Cluster topology">
        <div class="panel-head"><div class="title-with-help"><h2>Nodes</h2>${H("nodes-removal-help","About removing Nodes","Drain healthy Nodes before removal. Replace failed Nodes only after confirming the old process is permanently stopped.")}</div><span class="meta">${this.cluster?.members.length??0} members</span></div>
        ${this.cluster?.members.map(t=>this.renderClusterMember(t))}
        ${this.cluster?.members.length?g:c`<div class="empty">Cluster topology unavailable.</div>`}
      </section>
      <section class="panel" aria-label="Join tokens">
        <div class="panel-head"><h2>Join Tokens</h2><span class="meta">${this.joinTokens.length} stored</span></div>
        ${this.joinTokens.length?this.joinTokens.map(t=>c`
              <div class="resource">
                <div><strong>${t.id.slice(0,12)}…</strong><code>Expires ${new Date(t.expires_at_ms).toLocaleString()} · ${t.remaining_uses===null?"unlimited uses":`${t.remaining_uses} uses left`}</code></div>
                <button class="button danger" aria-label=${`Revoke Join Token ${t.id.slice(0,12)}`} @click=${()=>this.revokeJoinToken(t)}>Revoke</button>
              </div>
            `):c`<div class="empty">No Join Tokens.</div>`}
      </section>
      </div>
    `}renderTarget(t){const e=t.kind==="node",i=t.kind==="http",s=t.latest_evaluation,a=t.history.slice(0,16).reverse(),n=Math.max(1,...a.map(r=>r.latency_ms)),o=t.paused?"paused":t.availability==="down"?"down":t.consecutive_failures>0?"suspicious":t.availability;return c`
      <div class="target-wrap">
        ${e?c`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} disabled />`:c`<input class="select-target" type="checkbox" aria-label=${`Select ${t.name}`} .checked=${this.selectedIds.has(t.id)} @change=${r=>this.toggleSelected(t.id,r.target.checked)} />`}
        <button class=${`target ${e?"node-target":""}`} aria-label=${t.name} @click=${()=>this.openTarget(t)}>
          <i class="state ${o}" aria-label=${o}></i>
          <div>
            <div class="target-title"><h3>${t.name}</h3><span class="badge">${e?"Node":t.kind.toUpperCase()}</span></div>
            <div class="meta">${t.paused?"Paused · ":""}${i||e?`${t.method} · `:""}${t.url} · every ${t.interval_seconds}s${e?"":` · ${t.locations} ${t.locations===1?"location":"locations"}`}</div>
          </div>
          <div class="target-side">
            ${a.length?c`<div class="mini-chart" aria-hidden="true">${a.map(r=>c`<i class="mini-bar ${r.succeeded?"up":"down"}" style=${`height: ${Math.max(12,r.latency_ms/n*100)}%`}></i>`)}</div>`:g}
            <div class="latency">
              <strong>${s?`${s.latency_ms} ms`:"—"}</strong>
              <span>${s?i?s.status_code??"network error":s.succeeded?"reachable":"unreachable":"waiting"}</span>
            </div>
          </div>
        </button>
      </div>
    `}};Ue.styles=ve`
    :host {
      color-scheme: dark;
      --bg: #090d0c;
      --panel: #111715;
      --panel-2: #151d1a;
      --line: #27322e;
      --muted: #8fa099;
      --text: #edf7f2;
      --green: #58e29c;
      --red: #ff7575;
      --amber: #f2c264;
      --page-background:
        radial-gradient(circle at 12% -5%, #18392d 0, transparent 30%),
        linear-gradient(145deg, #090d0c 0%, #0c1210 55%, #09100d 100%);
      --brand-shadow: #40d89035;
      --nav-bg: #0d1210aa;
      --active-bg: #202b27;
      --button-border: #3e765a;
      --button-bg: #1c4a35;
      --button-text: #e8fff2;
      --button-hover-border: #62b988;
      --panel-surface: #111715dc;
      --panel-shadow: #0002;
      --divider: #202925;
      --badge-border: #3c554a;
      --badge-text: #a7c3b7;
      --row-hover: #17201c;
      --notice-border: #7b3937;
      --notice-bg: #391b1a;
      --notice-text: #ffb3af;
      --bulk-bg: #16221d;
      --dialog-shadow: #000b;
      --backdrop: #040706cc;
      --input-bg: #0c110f;
      --focus: #4b936c;
      --danger-text: #ff9b97;
      --danger-border: #633b39;
      --warning-bg: #594315;
      --warning-text: #ffd778;
      --warning-border: #9c7625;
      --join-bg: #0b110e;
      display: flex; flex-direction: column;
      min-height: 100vh;
      background: var(--page-background);
      color: var(--text);
      font: 14px/1.5 Inter, ui-sans-serif, system-ui, sans-serif;
      transition: background 220ms ease, color 180ms ease;
    }
    * { box-sizing: border-box; }
    button, input, select { font: inherit; }
    .shell { flex: 1 0 auto; width: 100%; max-width: 1200px; margin: auto; padding: 28px 24px 48px; }
    .setup-shell { display: grid; grid-template-rows: auto minmax(0, 1fr); padding-top: 20px; padding-bottom: 20px; }
    .setup-shell header { margin-bottom: 18px; } .setup-shell upgrid-setup { align-self: center; }
    header { display: grid; grid-template-columns: minmax(0, 1fr) auto minmax(0, 1fr); align-items: center; margin-bottom: 34px; }
    .brand, .actions, .live, nav { display: flex; align-items: center; }
    .brand { gap: 13px; }
    header > .brand { justify-self: start; }
    header > nav { justify-self: center; }
    header > .actions { justify-self: end; }
    .brand-line { display: flex; align-items: center; gap: 12px; }
    .brand img { width: 42px; height: 42px; filter: drop-shadow(0 0 18px var(--brand-shadow)); }
    .brand strong { display: block; font-size: 19px; letter-spacing: .02em; }
    .brand span, .live, .eyebrow, .meta { color: var(--muted); font-size: 12px; }
    nav { gap: 4px; padding: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); }
    nav a { color: var(--muted); padding: 7px 11px; text-decoration: none; border-radius: 7px; transition: background-color 160ms ease, color 160ms ease; }
    nav a.active { color: var(--text); background: var(--active-bg); }
    .actions { gap: 12px; }
    .live { gap: 7px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: var(--amber); transition: background-color 160ms ease, box-shadow 160ms ease; }
    .dot.up { background: var(--green); box-shadow: 0 0 10px var(--green); }
    .dot.degraded { background: var(--amber); box-shadow: 0 0 10px var(--amber); }
    .dot.down { background: var(--red); box-shadow: 0 0 10px var(--red); }
    .heading { display: flex; align-items: flex-end; justify-content: space-between; margin-bottom: 30px; }
    .heading h1 { margin: 2px 0 0; font-size: clamp(27px, 4vw, 38px); line-height: 1.1; letter-spacing: -.035em; }
    .eyebrow { text-transform: uppercase; letter-spacing: .16em; }
    .button { min-height: 44px; border: 1px solid var(--button-border); border-radius: 9px; background: var(--button-bg); color: var(--button-text); padding: 9px 13px; white-space: nowrap; cursor: pointer; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease, transform 120ms ease; }
    .button:hover { border-color: var(--button-hover-border); }
    .button:active { transform: translateY(1px); }
    .button:disabled { cursor: not-allowed; opacity: .65; }
    .button[aria-busy="true"] { cursor: wait; }
    .icon-button { display: grid; width: 44px; height: 44px; min-height: 44px; place-items: center; padding: 0; }
    iconify-icon { display: inline-block; width: 18px; height: 18px; font-size: 18px; }
    .account-menu { position: relative; }
    .account-menu summary { list-style: none; }
    .account-menu summary::-webkit-details-marker { display: none; }
    .account-dropdown { position: absolute; top: calc(100% + 8px); right: 0; z-index: 20; display: grid; width: max-content; min-width: 180px; gap: 2px; border: 1px solid var(--line); border-radius: 10px; background: var(--panel); padding: 6px; box-shadow: 0 16px 40px var(--dialog-shadow); }
    .account-dropdown .button { display: flex; width: 100%; min-height: 44px; align-items: center; justify-content: flex-start; box-sizing: border-box; border: 0; background: transparent; padding: 9px 13px; color: var(--muted); font: inherit; line-height: 1.2; text-align: left; text-decoration: none; }
    .account-dropdown .button:hover, .account-dropdown .button:focus-visible { background: var(--row-hover); color: var(--text); }
    .account-separator { height: 1px; margin: 4px 0; background: var(--divider); }
    .account-dropdown .danger { color: var(--danger-text); }
    .account-dropdown .danger:hover, .account-dropdown .danger:focus-visible { background: var(--notice-bg); color: var(--danger-text); }
    ${Zs}
    .auth-panel { width: min(440px, 100%); margin: auto; }
    .admin-page { width: min(760px, 100%); margin: auto; }
    .token-value { margin: 14px; overflow-wrap: anywhere; }
    .token-value code { display: block; margin: 8px 0; }
    .overview-top { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; margin-bottom: 18px; }
    .page-columns { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }
    .summary { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .metric, .panel { border: 1px solid var(--line); background: var(--panel-surface); box-shadow: 0 16px 48px var(--panel-shadow); transition: background-color 180ms ease, border-color 180ms ease, box-shadow 180ms ease; }
    .metric { border-radius: 14px; padding: 17px 18px; }
    .metric span { display: block; color: var(--muted); font-size: 11px; letter-spacing: .11em; text-transform: uppercase; }
    .metric strong { display: block; margin-top: 5px; font-size: 29px; font-weight: 560; }
    .metric.down.active span, .metric.down.active strong { color: var(--red); }
    .panel { border-radius: 16px; overflow: hidden; }
    .resource { display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 13px 20px; border-bottom: 1px solid var(--divider); }
    .resource:last-child { border-bottom: 0; }
    .resource strong { display: block; font-size: 13px; }
    .resource code { color: var(--muted); font-size: 11px; }
    .user-resource { padding: 0 20px 0 0; transition: background-color 150ms ease; }
    .user-resource:hover { background: var(--row-hover); }
    .resource-main { display: block; min-width: 0; flex: 1; border: 0; background: transparent; color: var(--text); padding: 13px 20px; text-align: left; }
    .badge { border: 1px solid var(--badge-border); border-radius: 999px; color: var(--badge-text); padding: 2px 7px; font-size: 10px; text-transform: uppercase; }
    .badge.up { border-color: var(--green); color: var(--green); }
    .badge.down { border-color: var(--red); color: var(--red); }
    .transition-main { display: flex; align-items: center; gap: 12px; }
    .channel-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .channel-summary { min-width: 0; }
    .channel-title, .channel-actions { display: flex; align-items: center; gap: 10px; }
    .channel-summary code { display: block; margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .channel-actions .switch span { font-size: 12px; }
    .panel-head { display: flex; align-items: center; justify-content: space-between; padding: 17px 20px; border-bottom: 1px solid var(--line); }
    .panel-head h2 { margin: 0; font-size: 14px; }
    .alert-history { margin-bottom: 20px; }
    .alert-filters { display: grid; grid-template-columns: minmax(180px, 1fr) repeat(3, minmax(120px, auto)); gap: 10px; padding: 14px 20px; border-bottom: 1px solid var(--line); }
    .alert-filters label { display: grid; gap: 5px; color: var(--muted); font-size: 11px; }
    .alert-resource { display: grid; grid-template-columns: minmax(0, 1fr) auto; }
    .alert-summary { display: grid; min-width: 0; gap: 4px; }
    .alert-summary code, .alert-summary .meta { font-size: 11px; }
    .alert-summary .meta { color: var(--muted); white-space: normal; }
    .alert-actions { display: flex; align-items: center; gap: 8px; }
    .target-wrap { display: grid; grid-template-columns: auto minmax(0, 1fr); align-items: center; border-bottom: 1px solid var(--divider); padding-left: 20px; }
    .target-wrap:last-child { border-bottom: 0; }
    .select-target { width: 18px; min-height: 18px; height: 18px; margin: 0; accent-color: var(--button-bg); cursor: pointer; }
    .target { width: 100%; display: grid; grid-template-columns: auto minmax(0, 1fr) auto; gap: 14px; align-items: center; padding: 17px 20px 17px 14px; border: 0; background: transparent; color: var(--text); text-align: left; cursor: pointer; }
    .target-wrap, .target { transition: background-color 150ms ease; }
    .target-wrap:hover, .target-wrap:hover .target { background: var(--row-hover); }
    .node-target { cursor: pointer; }
    .state { width: 10px; height: 10px; border-radius: 50%; color: var(--amber); background: var(--amber); box-shadow: 0 0 12px currentColor; transition: background-color 160ms ease, color 160ms ease, box-shadow 160ms ease; }
    .state.up { color: var(--green); background: var(--green); }
    .state.down { color: var(--red); background: var(--red); }
    .state.paused { color: var(--muted); background: var(--muted); box-shadow: none; }
    .target h3 { margin: 0 0 3px; font-size: 14px; }
    .target-title { display: flex; align-items: center; gap: 8px; margin-bottom: 3px; }
    .target-title h3 { margin: 0; }
    .meta { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .latency { text-align: right; }
    .latency strong { display: block; font-weight: 500; }
    .latency span { color: var(--muted); font-size: 11px; }
    .target-side { display: flex; align-items: center; gap: 20px; }
    .mini-chart { display: flex; width: 88px; height: 32px; align-items: flex-end; gap: 2px; }
    .mini-bar { flex: 1; min-width: 2px; max-width: 7px; border-radius: 2px 2px 1px 1px; opacity: .75; transition: background-color 160ms ease, height 180ms ease, opacity 160ms ease; }
    .mini-bar.up { background: var(--green); }
    .mini-bar.down { background: var(--red); }
    .empty { padding: 54px 20px; color: var(--muted); text-align: center; }
    .notice { margin: 0 0 16px; border: 1px solid var(--notice-border); border-radius: 10px; background: var(--notice-bg); color: var(--notice-text); padding: 10px 12px; }
    .toolbar { display: grid; grid-template-columns: minmax(180px, 1fr) auto auto; gap: 8px; padding: 12px 20px; border-bottom: 1px solid var(--line); }
    .toolbar input, .toolbar select { padding: 7px 9px; }
    .bulk { display: flex; align-items: center; gap: 8px; padding: 10px 20px; border-bottom: 1px solid var(--line); background: var(--bulk-bg); }
    .bulk-actions { display: flex; align-items: center; gap: 8px; margin-left: auto; }
    .bulk, .bulk-actions .button { animation: reveal 160ms ease-out; }
    @keyframes reveal { from { opacity: 0; transform: translateY(-3px); } }
    dialog { width: min(580px, calc(100% - 28px)); border: 1px solid var(--line); border-radius: 17px; background: var(--panel); color: var(--text); padding: 0; scrollbar-gutter: stable both-edges; box-shadow: 0 28px 90px var(--dialog-shadow); opacity: 0; transform: translateY(8px) scale(.985); transition: opacity 170ms ease, transform 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    #target-dialog { width: min(720px, calc(100% - 28px)); }
    dialog[open] { opacity: 1; transform: translateY(0) scale(1); }
    dialog::backdrop { background: var(--backdrop); backdrop-filter: blur(5px); opacity: 0; transition: opacity 170ms ease, overlay 170ms allow-discrete, display 170ms allow-discrete; }
    dialog[open]::backdrop { opacity: 1; }
    @starting-style {
      dialog[open] { opacity: 0; transform: translateY(8px) scale(.985); }
      dialog[open]::backdrop { opacity: 0; }
    }
    .dialog-head { position: relative; padding: 20px 58px 15px 22px; border-bottom: 1px solid var(--line); }
    .dialog-head h2 { margin: 0; font-size: 18px; }
    .dialog-head p { margin: 4px 0 0; color: var(--muted); }
    .target-dialog-head { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding-top: 12px; padding-bottom: 12px; }
    .target-dialog-head h2 { flex: none; }
    form { display: grid; gap: 13px; padding: 20px 22px 22px; }
    .form-tabs { display: flex; width: fit-content; min-width: 0; max-width: 100%; gap: 4px; border: 1px solid var(--line); border-radius: 11px; background: var(--nav-bg); padding: 4px; overflow-x: auto; }
    .form-tabs button { min-height: 34px; border: 0; border-radius: 7px; background: transparent; color: var(--muted); padding: 7px 11px; white-space: nowrap; cursor: pointer; transition: background-color 160ms ease, color 160ms ease; }
    .form-tabs button[aria-selected="true"] { background: var(--active-bg); color: var(--text); }
    .form-tabs button:disabled { cursor: not-allowed; opacity: .45; }
    .target-tab-panel { display: grid; gap: 13px; min-height: 190px; align-content: start; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 11px; }
    label { display: grid; gap: 6px; color: var(--muted); font-size: 14px; }
    [hidden] { display: none !important; }
    input, select { width: 100%; min-height: 44px; border: 1px solid var(--line); border-radius: 9px; outline: 0; background: var(--input-bg); color: var(--text); padding: 9px 10px; font-size: 16px; transition: background-color 160ms ease, border-color 160ms ease, color 160ms ease, opacity 160ms ease; }
    input:focus, select:focus { border-color: var(--focus); }
    button:focus-visible, a:focus-visible, .target:focus-visible, input:focus-visible, select:focus-visible { outline: 2px solid var(--green); outline-offset: 2px; }
    button, a, summary, [role="button"], [role="tab"], input[type="checkbox"], input[type="radio"], select, .target, .switch, .checkbox-option { cursor: pointer; user-select: none; }
    button:disabled, input:disabled, select:disabled { cursor: not-allowed; }
    input:disabled { cursor: not-allowed; opacity: .5; }
    .dialog-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 5px; }
    .danger-actions { display: flex; gap: 8px; margin-right: auto; }
    .secondary { background: transparent; color: var(--muted); border-color: var(--line); }
    .danger { background: transparent; color: var(--danger-text); border-color: var(--danger-border); }
    .warning { background: transparent; color: var(--warning-text); border-color: var(--warning-border); }
    .warning:hover { border-color: var(--warning-text); }
    .success { background: transparent; color: var(--green); border-color: var(--green); }
    .success:hover { border-color: var(--button-text); }
    .dialog-close { position: absolute; top: 12px; right: 14px; }
    .switch { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .channel-fields, .tls-fields { display: grid; gap: 10px; margin: 8px 0 0; border: 0; padding: 0; }
    .tls-fields legend { display: flex; width: 100%; align-items: center; gap: 12px; margin: 0 0 4px; padding: 0; color: var(--text); font-size: 14px; font-weight: 400; text-align: center; }
    .tls-fields legend::before, .tls-fields legend::after { height: 1px; flex: 1; background: var(--line); content: ""; }
    .tls-fields .meta { white-space: normal; }
    form .badge { font-size: 12px; }
    .channel-options { display: grid; gap: 6px; }
    .channel-options .checkbox-option { min-height: 36px; border-radius: 8px; padding: 5px 8px; background: var(--panel-2); }
    .switch-label { display: flex; min-width: 0; align-items: center; gap: 8px; }
    .switch-label .badge { margin-left: 0; }
    .checkbox-option { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .checkbox-control { width: 18px; min-height: 18px; height: 18px; flex: none; accent-color: var(--button-bg); cursor: pointer; }
    .switch-control { width: 42px; min-height: 24px; height: 24px; flex: none; appearance: none; border-radius: 999px; background: var(--input-bg); padding: 2px; cursor: pointer; }
    .switch-control::after { display: block; width: 16px; height: 16px; border-radius: 50%; background: var(--muted); content: ""; transition: background-color 160ms ease, transform 160ms ease; }
    .switch-control:checked { border-color: var(--button-border); background: var(--button-bg); }
    .switch-control:checked::after { background: var(--button-text); transform: translateX(18px); }
    footer { display: flex; flex: 0 0 auto; width: calc(100% - 48px); max-width: 1152px; flex-direction: column; align-items: center; justify-content: center; gap: 8px; margin: 0 auto; border-top: 1px solid var(--line); padding: 20px 0 24px; color: var(--muted); font-size: 12px; }
    .footer-links, .footer-powered { display: flex; align-items: center; justify-content: center; flex-wrap: wrap; gap: 10px; text-align: center; }
    footer a { display: inline-flex; align-items: center; gap: 4px; border-radius: 4px; color: var(--muted); text-decoration: underline; text-decoration-thickness: 1px; text-underline-offset: 3px; transition: color 160ms ease; }
    footer a:hover { color: var(--text); }
    footer iconify-icon { width: 14px; height: 14px; font-size: 14px; }
    .history { margin: 0 22px 22px; border-top: 1px solid var(--line); padding-top: 18px; }
    .detail-form .details-panel { gap: 0; }
    .detail-form .history { margin: 0; }
    .detail-form .history:first-child { border-top: 0; padding-top: 0; }
    .detail-form .history + .history { margin-top: 18px; }
    .history-head, .chart-legend, .chart-legend span, .chart-axis { display: flex; align-items: center; }
    .history-head { justify-content: space-between; margin-bottom: 12px; }
    .history-head h3 { margin: 0; font-size: 14px; }
    .history-summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
    .history-summary div { padding: 12px; border: 1px solid var(--line); border-radius: 9px; background: var(--input-bg); }
    .history-summary span { display: block; color: var(--muted); font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
    .history-summary strong { display: block; margin-top: 5px; font-size: 18px; font-weight: 560; }
    .chart-plot { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 7px; }
    .chart-scale { display: flex; height: 140px; flex-direction: column; justify-content: space-between; padding: 1px 0 7px; color: var(--muted); font-size: 9px; text-align: right; }
    .history-chart { display: flex; height: 140px; align-items: flex-end; gap: 3px; padding: 14px 10px 8px; border: 1px solid var(--line); border-radius: 10px; background: var(--input-bg); }
    .history-bar { flex: 1; min-width: 3px; max-width: 16px; border-radius: 3px 3px 1px 1px; opacity: .82; transform-origin: bottom; transition: opacity 120ms ease, transform 120ms ease; }
    .history-bar:hover { opacity: 1; transform: scaleX(1.15); }
    .history-bar.up { background: var(--green); }
    .history-bar.down { background: var(--red); }
    .chart-axis { justify-content: space-between; margin: 5px 0 0 45px; color: var(--muted); font-size: 10px; }
    .chart-legend { justify-content: flex-end; gap: 12px; margin-top: 9px; color: var(--muted); font-size: 10px; }
    .chart-legend span { gap: 5px; }
    .chart-legend i { width: 7px; height: 7px; border-radius: 2px; }
    .chart-legend .up { background: var(--green); }
    .chart-legend .down { background: var(--red); }
    .join-command { margin: 20px 22px; border: 1px solid var(--line); border-radius: 10px; background: var(--join-bg); color: var(--green); padding: 13px; overflow-wrap: anywhere; font: 12px/1.6 ui-monospace, SFMono-Regular, monospace; }
    :host([data-theme="bright"]) {
        color-scheme: light;
        --bg: #f4f8f6;
        --panel: #ffffff;
        --panel-2: #eef5f1;
        --line: #d3dfd9;
        --muted: #5d6e66;
        --text: #16211c;
        --green: #087a49;
        --red: #c53434;
        --amber: #9a6700;
        --page-background:
          radial-gradient(circle at 12% -5%, #d9f2e4 0, transparent 32%),
          linear-gradient(145deg, #fbfdfc 0%, #f3f8f5 55%, #edf5f1 100%);
        --brand-shadow: #159e5240;
        --nav-bg: #ffffffcc;
        --active-bg: #e4efe9;
        --button-border: #16764b;
        --button-bg: #087a49;
        --button-text: #ffffff;
        --button-hover-border: #075f3a;
        --panel-surface: #ffffffeb;
        --panel-shadow: #2745381a;
        --divider: #e3ebe7;
        --badge-border: #a6beb2;
        --badge-text: #426356;
        --row-hover: #e9f4ee;
        --notice-border: #e2aaa6;
        --notice-bg: #fff0ef;
        --notice-text: #9f2922;
        --bulk-bg: #e8f4ed;
        --dialog-shadow: #233b3050;
        --backdrop: #17251f66;
        --input-bg: #ffffff;
        --focus: #168655;
        --danger-text: #b42318;
        --danger-border: #dda29d;
        --warning-bg: #fff1bd;
        --warning-text: #805b00;
        --warning-border: #d4aa36;
        --join-bg: #eef8f2;
    }
    @media (prefers-reduced-motion: reduce) {
      :host, nav a, .button, .metric, .panel, .target-wrap, .target, .dot, .state, .mini-bar, .history-bar, dialog, dialog::backdrop, input, select, .help-tooltip-trigger, .help-tooltip { transition-duration: 0s; }
      .bulk, .bulk-actions .button { animation-duration: 0s; }
    }
    @media (max-width: 720px) {
      .shell { padding: 20px 14px 60px; }
      header { grid-template-columns: minmax(0, 1fr) auto; row-gap: 16px; }
      header > nav { display: flex; grid-column: 1 / -1; grid-row: 2; justify-self: center; }
      .overview-top { grid-template-columns: 1fr; }
      .page-columns { grid-template-columns: 1fr; }
      .toolbar { grid-template-columns: 1fr 1fr; }
      .toolbar input { grid-column: 1 / -1; }
      .heading { align-items: flex-start; gap: 16px; }
      .target-wrap { align-items: start; padding-left: 14px; } .select-target { align-self: start; margin-top: 6px; } .target { grid-template-columns: auto minmax(0, 1fr); gap: 10px; padding: 12px 14px 12px 10px; }
      .target-side { grid-column: 2; display: grid; grid-template-columns: minmax(88px, 1fr) auto; width: 100%; gap: 18px; margin-top: 4px; } .target > .state { align-self: start; margin-top: 5px; } .mini-chart { width: 100%; max-width: 140px; height: 28px; }
      .latency { min-width: 72px; text-align: right; }
      .channel-resource { grid-template-columns: 1fr; }
      .alert-filters { grid-template-columns: 1fr 1fr; }
      .alert-resource { grid-template-columns: 1fr; }
      .alert-actions { margin-top: 8px; }
      .channel-actions { justify-content: space-between; margin-top: 10px; }
      .target-dialog-head { gap: 8px; }
      .form-tabs { gap: 0; padding: 2px; }
      .form-tabs button { min-height: 30px; padding: 5px 2px; font-size: 12px; }
    }
  `;Ue=la([Be("upgrid-app")],Ue);
